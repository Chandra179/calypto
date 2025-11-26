package passkey

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"
)

// PasskeyHandler handles passkey registration and authentication
type PasskeyHandler struct {
	webAuthn *webauthn.WebAuthn
	storage  Storage
	config   Config
}

// Config holds configuration for passkey handler
type Config struct {
	RPDisplayName         string   // "Example Corp"
	RPID                  string   // "example.com"
	RPOrigins             []string // ["https://example.com"]
	Timeout               int      // Timeout in milliseconds (default: 60000)
	RequireResidentKey    bool     // Enable discoverable credentials (username-less login)
	UserVerification      string   // "required", "preferred", "discouraged"
	AttestationPreference string   // "none", "indirect", "direct", "enterprise"
	AuthenticatorType     string   // "platform", "cross-platform", "" (both)
}

// DefaultConfig returns a secure default configuration
func DefaultConfig() Config {
	return Config{
		RPDisplayName:         "Passkey App",
		RPID:                  "localhost",
		RPOrigins:             []string{"http://localhost:8080"},
		Timeout:               60000,
		RequireResidentKey:    false,
		UserVerification:      "required",
		AttestationPreference: "direct",
		AuthenticatorType:     "", // Allow both platform and cross-platform
	}
}

// NewPasskeyHandler creates a new passkey handler
func NewPasskeyHandler(config Config, storage Storage) (*PasskeyHandler, error) {
	wconfig := &webauthn.Config{
		RPDisplayName: config.RPDisplayName,
		RPID:          config.RPID,
		RPOrigins:     config.RPOrigins,
		Timeouts: webauthn.TimeoutsConfig{
			Login: webauthn.TimeoutConfig{
				Enforce:    true,
				Timeout:    time.Duration(config.Timeout) * time.Millisecond,
				TimeoutUVD: time.Duration(config.Timeout) * time.Millisecond,
			},
			Registration: webauthn.TimeoutConfig{
				Enforce:    true,
				Timeout:    time.Duration(config.Timeout) * time.Millisecond,
				TimeoutUVD: time.Duration(config.Timeout) * time.Millisecond,
			},
		},
	}

	web, err := webauthn.New(wconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create webauthn: %w", err)
	}

	return &PasskeyHandler{
		webAuthn: web,
		storage:  storage,
		config:   config,
	}, nil
}

// BeginRegistration starts the registration process
func (h *PasskeyHandler) BeginRegistration(c *gin.Context) {
	var req struct {
		Username string `json:"username" binding:"required"`
		Nickname string `json:"nickname"` // Optional nickname for the credential
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request"})
		return
	}

	// Validate username
	if len(req.Username) < 3 || len(req.Username) > 64 {
		c.JSON(400, gin.H{"error": "Username must be between 3 and 64 characters"})
		return
	}

	// Try to get existing user or create new one
	user, err := h.storage.GetUser(req.Username)
	if err != nil {
		user, err = h.storage.CreateUser(req.Username)
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to create user"})
			return
		}
	}

	// Build authenticator selection criteria
	authenticatorSelection := protocol.AuthenticatorSelection{
		UserVerification: protocol.UserVerificationRequirement(h.config.UserVerification),
	}

	if h.config.RequireResidentKey {
		residentKey := protocol.ResidentKeyRequirementRequired
		authenticatorSelection.ResidentKey = residentKey
		authenticatorSelection.RequireResidentKey = protocol.ResidentKeyRequired()
	} else {
		residentKey := protocol.ResidentKeyRequirementDiscouraged
		authenticatorSelection.ResidentKey = residentKey
		authenticatorSelection.RequireResidentKey = protocol.ResidentKeyNotRequired()
	}

	if h.config.AuthenticatorType != "" {
		attachment := protocol.AuthenticatorAttachment(h.config.AuthenticatorType)
		authenticatorSelection.AuthenticatorAttachment = attachment
	}

	// Convert credentials to descriptors for exclusion list
	exclusionList := make([]protocol.CredentialDescriptor, len(user.Credentials))
	for i, cred := range user.Credentials {
		exclusionList[i] = cred.Credential.Descriptor()
	}

	options, session, err := h.webAuthn.BeginRegistration(
		user,
		webauthn.WithAuthenticatorSelection(authenticatorSelection),
		webauthn.WithConveyancePreference(protocol.ConveyancePreference(h.config.AttestationPreference)),
		webauthn.WithExclusions(exclusionList),
	)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to begin registration"})
		return
	}

	sessionID := generateSessionID()
	if err := h.storage.SaveSession(sessionID, session); err != nil {
		c.JSON(500, gin.H{"error": "Failed to save session"})
		return
	}

	c.SetCookie("registration_session", sessionID, 300, "/", "", false, true)
	if req.Nickname != "" {
		c.SetCookie("credential_nickname", req.Nickname, 300, "/", "", false, true)
	}

	c.JSON(200, options)
}

// FinishRegistration completes the registration process
func (h *PasskeyHandler) FinishRegistration(c *gin.Context) {
	sessionID, err := c.Cookie("registration_session")
	if err != nil {
		c.JSON(400, gin.H{"error": "Session not found"})
		return
	}

	session, err := h.storage.GetSession(sessionID)
	if err != nil {
		c.JSON(400, gin.H{"error": "Invalid or expired session"})
		return
	}

	user, err := h.storage.GetUserByID(session.UserID)
	if err != nil {
		c.JSON(400, gin.H{"error": "User not found"})
		return
	}

	credential, err := h.webAuthn.FinishRegistration(user, *session, c.Request)
	if err != nil {
		c.JSON(400, gin.H{"error": "Registration verification failed"})
		return
	}

	// Get nickname from cookie if available
	nickname, _ := c.Cookie("credential_nickname")
	if nickname == "" {
		nickname = fmt.Sprintf("Passkey %d", len(user.Credentials)+1)
	}

	// Extract transports if available
	var transports []string
	if credential.Transport != nil {
		transports = make([]string, len(credential.Transport))
		for i, t := range credential.Transport {
			transports[i] = string(t)
		}
	}

	user.AddCredential(*credential, nickname, transports)
	if err := h.storage.UpdateUser(user); err != nil {
		c.JSON(500, gin.H{"error": "Failed to save credential"})
		return
	}

	h.storage.DeleteSession(sessionID)
	c.SetCookie("registration_session", "", -1, "/", "", false, true)
	c.SetCookie("credential_nickname", "", -1, "/", "", false, true)

	c.JSON(200, gin.H{
		"status":  "success",
		"message": "Registration completed",
		"credential": gin.H{
			"id":       base64.RawURLEncoding.EncodeToString(credential.ID),
			"nickname": nickname,
		},
	})
}

// BeginLogin starts the authentication process
func (h *PasskeyHandler) BeginLogin(c *gin.Context) {
	var req struct {
		Username string `json:"username"`
	}

	// Username is optional for discoverable credentials
	c.ShouldBindJSON(&req)

	var options *protocol.CredentialAssertion
	var session *webauthn.SessionData
	var err error

	if req.Username != "" {
		user, err := h.storage.GetUser(req.Username)
		if err != nil {
			c.JSON(404, gin.H{"error": "Authentication failed"})
			return
		}

		options, session, err = h.webAuthn.BeginLogin(user)
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to begin login"})
			return
		}
	} else {
		options, session, err = h.webAuthn.BeginDiscoverableLogin()
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to begin login"})
			return
		}
	}

	sessionID := generateSessionID()
	if err := h.storage.SaveSession(sessionID, session); err != nil {
		c.JSON(500, gin.H{"error": "Failed to save session"})
		return
	}

	c.SetCookie("login_session", sessionID, 300, "/", "", false, true)
	c.JSON(200, options)
}

// FinishLogin completes the authentication process
func (h *PasskeyHandler) FinishLogin(c *gin.Context) {
	sessionID, err := c.Cookie("login_session")
	if err != nil {
		c.JSON(400, gin.H{"error": "Session not found"})
		return
	}

	session, err := h.storage.GetSession(sessionID)
	if err != nil {
		c.JSON(400, gin.H{"error": "Invalid or expired session"})
		return
	}

	user, err := h.storage.GetUserByID(session.UserID)
	if err != nil {
		c.JSON(400, gin.H{"error": "Authentication failed"})
		return
	}

	credential, err := h.webAuthn.FinishLogin(user, *session, c.Request)
	if err != nil {
		c.JSON(400, gin.H{"error": "Authentication verification failed"})
		return
	}

	// Check for credential cloning (counter should always increment)
	existingCred := user.GetCredential(credential.ID)
	if existingCred != nil && credential.Authenticator.SignCount > 0 {
		if credential.Authenticator.SignCount <= existingCred.Authenticator.SignCount {
			c.JSON(400, gin.H{"error": "Credential cloning detected"})
			return
		}
	}

	user.UpdateCredential(*credential)
	if err := h.storage.UpdateUser(user); err != nil {
		c.JSON(500, gin.H{"error": "Failed to update credential"})
		return
	}

	h.storage.DeleteSession(sessionID)
	c.SetCookie("login_session", "", -1, "/", "", false, true)

	c.JSON(200, gin.H{
		"status":   "success",
		"message":  "Login completed",
		"username": user.Username,
		"credential": gin.H{
			"id":       base64.RawURLEncoding.EncodeToString(credential.ID),
			"nickname": existingCred.Nickname,
		},
	})
}

// ListCredentials returns all credentials for a user
func (h *PasskeyHandler) ListCredentials(c *gin.Context) {
	username := c.Param("username")
	if username == "" {
		c.JSON(400, gin.H{"error": "Username required"})
		return
	}

	user, err := h.storage.GetUser(username)
	if err != nil {
		c.JSON(404, gin.H{"error": "User not found"})
		return
	}

	credentials := make([]gin.H, len(user.Credentials))
	for i, cred := range user.Credentials {
		credentials[i] = gin.H{
			"id":           base64.RawURLEncoding.EncodeToString(cred.ID),
			"nickname":     cred.Nickname,
			"created_at":   cred.CreatedAt,
			"last_used_at": cred.LastUsedAt,
			"transports":   cred.Transports,
			"backup_state": cred.BackupState,
		}
	}

	c.JSON(200, gin.H{
		"username":    user.Username,
		"credentials": credentials,
	})
}

// DeleteCredential removes a credential
func (h *PasskeyHandler) DeleteCredential(c *gin.Context) {
	username := c.Param("username")
	credentialIDStr := c.Param("credentialId")

	if username == "" || credentialIDStr == "" {
		c.JSON(400, gin.H{"error": "Username and credential ID required"})
		return
	}

	credentialID, err := base64.RawURLEncoding.DecodeString(credentialIDStr)
	if err != nil {
		c.JSON(400, gin.H{"error": "Invalid credential ID"})
		return
	}

	user, err := h.storage.GetUser(username)
	if err != nil {
		c.JSON(404, gin.H{"error": "User not found"})
		return
	}

	if !user.DeleteCredential(credentialID) {
		c.JSON(404, gin.H{"error": "Credential not found"})
		return
	}

	if err := h.storage.UpdateUser(user); err != nil {
		c.JSON(500, gin.H{"error": "Failed to delete credential"})
		return
	}

	c.JSON(200, gin.H{
		"status":  "success",
		"message": "Credential deleted",
	})
}

// RenameCredential updates a credential's nickname
func (h *PasskeyHandler) RenameCredential(c *gin.Context) {
	username := c.Param("username")
	credentialIDStr := c.Param("credentialId")

	var req struct {
		Nickname string `json:"nickname" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Nickname required"})
		return
	}

	credentialID, err := base64.RawURLEncoding.DecodeString(credentialIDStr)
	if err != nil {
		c.JSON(400, gin.H{"error": "Invalid credential ID"})
		return
	}

	user, err := h.storage.GetUser(username)
	if err != nil {
		c.JSON(404, gin.H{"error": "User not found"})
		return
	}

	if !user.RenameCredential(credentialID, req.Nickname) {
		c.JSON(404, gin.H{"error": "Credential not found"})
		return
	}

	if err := h.storage.UpdateUser(user); err != nil {
		c.JSON(500, gin.H{"error": "Failed to rename credential"})
		return
	}

	c.JSON(200, gin.H{
		"status":  "success",
		"message": "Credential renamed",
	})
}

// RegisterRoutes registers all passkey routes to a Gin router
func (h *PasskeyHandler) RegisterRoutes(r *gin.Engine) {
	r.POST("/passkey/register/begin", h.BeginRegistration)
	r.POST("/passkey/register/finish", h.FinishRegistration)
	r.POST("/passkey/login/begin", h.BeginLogin)
	r.POST("/passkey/login/finish", h.FinishLogin)
	r.GET("/passkey/credentials/:username", h.ListCredentials)
	r.DELETE("/passkey/credentials/:username/:credentialId", h.DeleteCredential)
	r.PUT("/passkey/credentials/:username/:credentialId", h.RenameCredential)
}

func generateSessionID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}

// ServeHTML serves the test HTML page
func (h *PasskeyHandler) ServeHTML(c *gin.Context) {
	c.Header("Content-Type", "text/html; charset=utf-8")

	// Pass configuration to HTML
	configJSON, _ := json.Marshal(gin.H{
		"requireResidentKey": h.config.RequireResidentKey,
		"userVerification":   h.config.UserVerification,
		"authenticatorType":  h.config.AuthenticatorType,
	})

	html := fmt.Sprintf(htmlContent, string(configJSON))
	c.String(200, html)
}

const htmlContent = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Passkey Test</title>
    <style>
        * {
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%%, #764ba2 100%%);
            min-height: 100vh;
        }
        .container {
            background: white;
            padding: 40px;
            border-radius: 16px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
        }
        h1 {
            color: #333;
            margin: 0 0 10px 0;
            font-size: 32px;
        }
        h2 {
            color: #555;
            font-size: 20px;
            margin: 30px 0 15px 0;
            padding-top: 20px;
            border-top: 2px solid #f0f0f0;
        }
        h2:first-of-type {
            border-top: none;
            padding-top: 0;
        }
        .subtitle {
            color: #666;
            margin-bottom: 30px;
            font-size: 14px;
        }
        .config-info {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 25px;
            font-size: 13px;
            line-height: 1.6;
        }
        .config-info strong {
            color: #495057;
        }
        .config-badge {
            display: inline-block;
            background: #667eea;
            color: white;
            padding: 3px 8px;
            border-radius: 4px;
            font-size: 11px;
            margin-left: 5px;
            font-weight: 600;
        }
        input {
            width: 100%%;
            padding: 12px;
            margin: 8px 0;
            border: 2px solid #e1e4e8;
            border-radius: 8px;
            font-size: 15px;
            transition: border-color 0.2s;
        }
        input:focus {
            outline: none;
            border-color: #667eea;
        }
        button {
            background: linear-gradient(135deg, #667eea 0%%, #764ba2 100%%);
            color: white;
            padding: 14px 24px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            width: 100%%;
            font-weight: 600;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        button:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
        }
        button:active {
            transform: translateY(0);
        }
        button:disabled {
            background: #ccc;
            cursor: not-allowed;
            transform: none;
        }
        .message {
            margin-top: 15px;
            padding: 12px;
            border-radius: 8px;
            font-size: 14px;
        }
        .success {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        .error {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        .info {
            background: #d1ecf1;
            color: #0c5460;
            border: 1px solid #bee5eb;
        }
        .credentials-list {
            margin-top: 15px;
        }
        .credential-item {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .credential-info {
            flex: 1;
        }
        .credential-name {
            font-weight: 600;
            color: #333;
            margin-bottom: 5px;
        }
        .credential-meta {
            font-size: 12px;
            color: #666;
        }
        .credential-actions {
            display: flex;
            gap: 8px;
        }
        .btn-small {
            padding: 6px 12px;
            font-size: 13px;
            width: auto;
            background: #6c757d;
        }
        .btn-small.danger {
            background: #dc3545;
        }
        .btn-small:hover {
            transform: translateY(-1px);
        }
        .toggle-section {
            margin: 15px 0;
        }
        .toggle-btn {
            background: #6c757d;
            padding: 10px 20px;
            font-size: 14px;
            margin-bottom: 10px;
        }
        .hidden {
            display: none;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔐 Passkey Authentication</h1>
        <div class="subtitle">WebAuthn Test Interface</div>
        
        <div class="config-info" id="configInfo"></div>

        <h2>Register New Passkey</h2>
        <input type="text" id="registerUsername" placeholder="Enter username (3-64 characters)" />
        <input type="text" id="registerNickname" placeholder="Device nickname (optional, e.g., 'My iPhone')" />
        <button onclick="register()">Register with Passkey</button>
        <div id="registerMessage"></div>

        <h2>Login with Passkey</h2>
        <input type="text" id="loginUsername" placeholder="Enter username (leave empty for username-less login)" />
        <button onclick="login()">Login with Passkey</button>
        <div id="loginMessage"></div>

        <h2>Manage Credentials</h2>
        <input type="text" id="manageUsername" placeholder="Enter username" />
        <button onclick="loadCredentials()">Load My Passkeys</button>
        <div id="credentialsMessage"></div>
        <div id="credentialsList" class="credentials-list"></div>
    </div>

    <script>
        const API_BASE = window.location.origin;
        const CONFIG = %s;

        // Display configuration info
        window.onload = function() {
            let configHTML = '<strong>Configuration:</strong><br>';
            if (CONFIG.requireResidentKey) {
                configHTML += '✓ Discoverable Credentials (Username-less login) <span class="config-badge">ENABLED</span><br>';
            } else {
                configHTML += '○ Username required for login<br>';
            }
            configHTML += '✓ User Verification: <span class="config-badge">' + CONFIG.userVerification.toUpperCase() + '</span><br>';
            configHTML += '✓ Attestation: <span class="config-badge">DIRECT</span><br>';
            
            if (CONFIG.authenticatorType) {
                configHTML += '✓ Authenticator Type: <span class="config-badge">' + CONFIG.authenticatorType.toUpperCase() + '</span>';
            } else {
                configHTML += '✓ Authenticator Type: <span class="config-badge">ALL</span>';
            }
            
            document.getElementById('configInfo').innerHTML = configHTML;

            // Show hint about username-less login
            if (CONFIG.requireResidentKey) {
                document.getElementById('loginUsername').placeholder = 'Leave empty to use username-less login';
            }
        };

        function showMessage(elementId, message, type) {
            const el = document.getElementById(elementId);
            el.innerHTML = '<div class="message ' + type + '">' + message + '</div>';
        }

        function base64urlToBuffer(base64url) {
            const base64 = base64url.replace(/-/g, '+').replace(/_/g, '/');
            const padLen = (4 - (base64.length %% 4)) %% 4;
            const padded = base64 + '='.repeat(padLen);
            const binary = atob(padded);
            const bytes = new Uint8Array(binary.length);
            for (let i = 0; i < binary.length; i++) {
                bytes[i] = binary.charCodeAt(i);
            }
            return bytes.buffer;
        }

        function bufferToBase64url(buffer) {
            const bytes = new Uint8Array(buffer);
            let binary = '';
            for (let i = 0; i < bytes.length; i++) {
                binary += String.fromCharCode(bytes[i]);
            }
            return btoa(binary)
                .replace(/\+/g, '-')
                .replace(/\//g, '_')
                .replace(/=/g, '');
        }

        async function register() {
            const username = document.getElementById('registerUsername').value.trim();
            const nickname = document.getElementById('registerNickname').value.trim();
            
            if (!username) {
                showMessage('registerMessage', 'Please enter a username', 'error');
                return;
            }

            if (username.length < 3 || username.length > 64) {
                showMessage('registerMessage', 'Username must be between 3 and 64 characters', 'error');
                return;
            }

            try {
                showMessage('registerMessage', 'Starting registration...', 'info');

                const beginResp = await fetch(API_BASE + '/passkey/register/begin', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify({ 
                        username: username,
                        nickname: nickname || undefined
                    })
                });

                if (!beginResp.ok) {
                    const error = await beginResp.json();
                    throw new Error(error.error || 'Server error: ' + beginResp.status);
                }

                const options = await beginResp.json();

                options.publicKey.challenge = base64urlToBuffer(options.publicKey.challenge);
                options.publicKey.user.id = base64urlToBuffer(options.publicKey.user.id);

                if (options.publicKey.excludeCredentials) {
                    options.publicKey.excludeCredentials = options.publicKey.excludeCredentials.map(cred => ({
                        id: base64urlToBuffer(cred.id),
                        type: cred.type,
                        transports: cred.transports
                    }));
                }

                showMessage('registerMessage', '👆 Touch your authenticator...', 'info');

                const credential = await navigator.credentials.create(options);

                const credentialJSON = {
                    id: credential.id,
                    rawId: bufferToBase64url(credential.rawId),
                    type: credential.type,
                    response: {
                        attestationObject: bufferToBase64url(credential.response.attestationObject),
                        clientDataJSON: bufferToBase64url(credential.response.clientDataJSON)
                    }
                };

                const finishResp = await fetch(API_BASE + '/passkey/register/finish', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify(credentialJSON)
                });

                if (!finishResp.ok) {
                    const error = await finishResp.json();
                    throw new Error(error.error || 'Registration failed');
                }

                const result = await finishResp.json();
                showMessage('registerMessage', 
                    '✅ Registration successful! Credential: <strong>' + result.credential.nickname + '</strong>', 
                    'success');

                // Clear inputs
                document.getElementById('registerUsername').value = '';
                document.getElementById('registerNickname').value = '';

            } catch (error) {
                console.error('Registration error:', error);
                showMessage('registerMessage', '❌ Error: ' + error.message, 'error');
            }
        }

        async function login() {
            const username = document.getElementById('loginUsername').value.trim();

            try {
                showMessage('loginMessage', 'Starting login...', 'info');

                const body = username ? { username: username } : {};

                const beginResp = await fetch(API_BASE + '/passkey/login/begin', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify(body)
                });

                if (!beginResp.ok) {
                    const error = await beginResp.json();
                    throw new Error(error.error || 'Server error: ' + beginResp.status);
                }

                const options = await beginResp.json();

                options.publicKey.challenge = base64urlToBuffer(options.publicKey.challenge);

                if (options.publicKey.allowCredentials) {
                    options.publicKey.allowCredentials = options.publicKey.allowCredentials.map(cred => ({
                        id: base64urlToBuffer(cred.id),
                        type: cred.type,
                        transports: cred.transports
                    }));
                }

                showMessage('loginMessage', '👆 Touch your authenticator...', 'info');

                const credential = await navigator.credentials.get(options);

                const assertionJSON = {
                    id: credential.id,
                    rawId: bufferToBase64url(credential.rawId),
                    type: credential.type,
                    response: {
                        authenticatorData: bufferToBase64url(credential.response.authenticatorData),
                        clientDataJSON: bufferToBase64url(credential.response.clientDataJSON),
                        signature: bufferToBase64url(credential.response.signature),
                        userHandle: credential.response.userHandle ? bufferToBase64url(credential.response.userHandle) : null
                    }
                };

                const finishResp = await fetch(API_BASE + '/passkey/login/finish', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    credentials: 'include',
                    body: JSON.stringify(assertionJSON)
                });

                if (!finishResp.ok) {
                    const error = await finishResp.json();
                    throw new Error(error.error || 'Login failed');
                }

                const result = await finishResp.json();
                showMessage('loginMessage', 
                    '✅ Login successful! Welcome, <strong>' + result.username + '</strong>!<br>' +
                    'Used credential: <strong>' + result.credential.nickname + '</strong>', 
                    'success');

                // Clear input
                document.getElementById('loginUsername').value = '';

            } catch (error) {
                console.error('Login error:', error);
                showMessage('loginMessage', '❌ Error: ' + error.message, 'error');
            }
        }

        async function loadCredentials() {
            const username = document.getElementById('manageUsername').value.trim();
            
            if (!username) {
                showMessage('credentialsMessage', 'Please enter a username', 'error');
                return;
            }

            try {
                showMessage('credentialsMessage', 'Loading credentials...', 'info');

                const resp = await fetch(API_BASE + '/passkey/credentials/' + encodeURIComponent(username), {
                    method: 'GET',
                    credentials: 'include'
                });

                if (!resp.ok) {
                    const error = await resp.json();
                    throw new Error(error.error || 'Failed to load credentials');
                }

                const data = await resp.json();
                
                if (data.credentials.length === 0) {
                    showMessage('credentialsMessage', 'No passkeys found for this user', 'info');
                    document.getElementById('credentialsList').innerHTML = '';
                    return;
                }

                showMessage('credentialsMessage', 
                    '✅ Found ' + data.credentials.length + ' passkey(s)', 
                    'success');

                let html = '';
                data.credentials.forEach(cred => {
                    const createdDate = new Date(cred.created_at).toLocaleString();
                    const lastUsedDate = new Date(cred.last_used_at).toLocaleString();
                    const backupBadge = cred.backup_state ? 
                        '<span class="config-badge" style="background: #28a745;">SYNCED</span>' : 
                        '<span class="config-badge" style="background: #6c757d;">DEVICE-ONLY</span>';
                    
                    html += '<div class="credential-item">';
                    html += '<div class="credential-info">';
                    html += '<div class="credential-name">' + escapeHtml(cred.nickname) + ' ' + backupBadge + '</div>';
                    html += '<div class="credential-meta">';
                    html += 'Created: ' + createdDate + '<br>';
                    html += 'Last used: ' + lastUsedDate + '<br>';
                    html += 'ID: ' + cred.id.substring(0, 16) + '...';
                    if (cred.transports && cred.transports.length > 0) {
                        html += '<br>Transports: ' + cred.transports.join(', ');
                    }
                    html += '</div></div>';
                    html += '<div class="credential-actions">';
                    html += '<button class="btn-small" onclick="renameCredential(\'' + username + '\', \'' + cred.id + '\', \'' + escapeHtml(cred.nickname) + '\')">Rename</button>';
                    html += '<button class="btn-small danger" onclick="deleteCredential(\'' + username + '\', \'' + cred.id + '\', \'' + escapeHtml(cred.nickname) + '\')">Delete</button>';
                    html += '</div></div>';
                });

                document.getElementById('credentialsList').innerHTML = html;

            } catch (error) {
                console.error('Load credentials error:', error);
                showMessage('credentialsMessage', '❌ Error: ' + error.message, 'error');
                document.getElementById('credentialsList').innerHTML = '';
            }
        }

        async function deleteCredential(username, credId, nickname) {
            if (!confirm('Are you sure you want to delete "' + nickname + '"?')) {
                return;
            }

            try {
                const resp = await fetch(
                    API_BASE + '/passkey/credentials/' + 
                    encodeURIComponent(username) + '/' + 
                    encodeURIComponent(credId),
                    {
                        method: 'DELETE',
                        credentials: 'include'
                    }
                );

                if (!resp.ok) {
                    const error = await resp.json();
                    throw new Error(error.error || 'Failed to delete credential');
                }

                showMessage('credentialsMessage', '✅ Credential deleted successfully', 'success');
                
                // Reload credentials list
                loadCredentials();

            } catch (error) {
                console.error('Delete credential error:', error);
                showMessage('credentialsMessage', '❌ Error: ' + error.message, 'error');
            }
        }

        async function renameCredential(username, credId, currentNickname) {
            const newNickname = prompt('Enter new nickname for this passkey:', currentNickname);
            
            if (!newNickname || newNickname === currentNickname) {
                return;
            }

            try {
                const resp = await fetch(
                    API_BASE + '/passkey/credentials/' + 
                    encodeURIComponent(username) + '/' + 
                    encodeURIComponent(credId),
                    {
                        method: 'PUT',
                        headers: { 'Content-Type': 'application/json' },
                        credentials: 'include',
                        body: JSON.stringify({ nickname: newNickname })
                    }
                );

                if (!resp.ok) {
                    const error = await resp.json();
                    throw new Error(error.error || 'Failed to rename credential');
                }

                showMessage('credentialsMessage', '✅ Credential renamed successfully', 'success');
                
                // Reload credentials list
                loadCredentials();

            } catch (error) {
                console.error('Rename credential error:', error);
                showMessage('credentialsMessage', '❌ Error: ' + error.message, 'error');
            }
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
    </script>
</body>
</html>`
