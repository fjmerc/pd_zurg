#!/bin/bash
#
# pd_zurg quick setup — configures docker-compose.yml for first run.
#
# Sets up the web-based settings editor so you can configure everything
# else from your browser at http://localhost:8080/settings
#

DOCKER_COMPOSE_FILE="docker-compose.yml"

if [[ ! -f "$DOCKER_COMPOSE_FILE" ]]; then
    echo "Error: $DOCKER_COMPOSE_FILE not found in current directory."
    echo "Run this script from the directory containing your docker-compose.yml."
    exit 1
fi

echo ""
echo "  pd_zurg Quick Setup"
echo "  ==================="
echo ""
echo "  This will configure the web-based settings editor so you can"
echo "  manage all settings from your browser."
echo ""

# Ask for auth credentials
read -p "  Choose a username for the web UI [admin]: " ui_user
ui_user=${ui_user:-admin}

while true; do
    read -s -p "  Choose a password for the web UI: " ui_pass
    echo ""
    if [[ -z "$ui_pass" ]]; then
        echo "  Password cannot be empty. Please try again."
    else
        break
    fi
done

# Ask for the essentials — API key
echo ""
read -p "  Do you have a Real-Debrid API key? (y/n) [n]: " has_rd
has_rd=$(echo "$has_rd" | tr '[:upper:]' '[:lower:]')

rd_key=""
if [[ "$has_rd" == "y" || "$has_rd" == "yes" ]]; then
    read -p "  Enter your Real-Debrid API key: " rd_key
fi

# Apply changes to docker-compose.yml
escape_for_sed() {
    echo "$1" | sed -e 's/[\/&]/\\&/g'
}

set_env_var() {
    local var=$1
    local val=$(escape_for_sed "$2")
    if grep -q "^\([[:space:]]*\)#\s*-\s*$var=" "$DOCKER_COMPOSE_FILE"; then
        sed -i "s|^\([[:space:]]*\)#\s*-\s*$var=.*|\1 - $var=$val|" "$DOCKER_COMPOSE_FILE"
    elif grep -q "^\([[:space:]]*\)-\s*$var=" "$DOCKER_COMPOSE_FILE"; then
        sed -i "s|^\([[:space:]]*\)-\s*$var=.*|\1- $var=$val|" "$DOCKER_COMPOSE_FILE"
    fi
}

# Enable Status UI (should already be enabled in default compose)
set_env_var "STATUS_UI_ENABLED" "true"
set_env_var "STATUS_UI_AUTH" "$ui_user:$ui_pass"

# Set API key and enable Zurg if provided
if [[ -n "$rd_key" ]]; then
    set_env_var "ZURG_ENABLED" "true"
    set_env_var "RD_API_KEY" "$rd_key"
fi

# Get the port
ui_port=$(grep -oP 'STATUS_UI_PORT=\K[0-9]+' "$DOCKER_COMPOSE_FILE" 2>/dev/null || echo "8080")
ui_port=${ui_port:-8080}

echo ""
echo "  Setup complete!"
echo ""
echo "  Next steps:"
echo "    1. Start the container:  docker compose up -d"
echo "    2. Open your browser:    http://localhost:$ui_port/settings"
echo "    3. Log in with:          $ui_user / (your password)"
echo "    4. Configure everything else from the web UI"
echo ""
if [[ -z "$rd_key" ]]; then
    echo "  Note: You'll need to add your debrid API key in the web settings"
    echo "  editor under the Zurg category before services will start."
    echo ""
fi
