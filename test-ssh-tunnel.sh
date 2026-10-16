#!/bin/bash
set -e

echo "====== SSH Tunnel Test for BitMagnet ======"
echo "This test will check if we can connect to the remote PostgreSQL server"

# Test 1: Check if SSH alias is properly configured
echo -e "\n1. Checking SSH configuration..."
grep -q "Host alberto-hetzner" ~/.ssh/config 2>/dev/null && echo "SSH alias found in config" || echo "Warning: SSH alias not found in ~/.ssh/config"

echo -e "\n2. Testing basic SSH connectivity..."
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no alberto-hetzner 'echo "✅ SSH connection successful"' || 
  { echo "❌ SSH connection failed. Please verify your SSH setup and try again."; }

echo -e "\n3. SSH connection details for debugging:"
ssh -v -o BatchMode=yes -o ConnectTimeout=3 -o StrictHostKeyChecking=no alberto-hetzner 'hostname' 2>&1 | grep -E 'debug1|Connection|^debug2'

# If SSH is successful, check PostgreSQL connectivity
if command -v psql &> /dev/null; then
  echo -e "\n4. Testing direct PostgreSQL connectivity..."
  PGPASSWORD=postgres psql -h 192.168.55.11 -U postgres -d bitmagnet -c 'SELECT version()' 2>/dev/null && 
    echo "✅ PostgreSQL connection successful" || 
    echo "❌ PostgreSQL connection failed"
else
  echo -e "\n4. PostgreSQL client not available. Skipping database test."
fi

echo -e "\n====== Test Summary ======"
echo "For BitMagnet to work with the remote database:"
echo "1. SSH connection must work with your alias or IP address"
echo "2. PostgreSQL must be accessible through that connection"
echo ""
echo "Configuration in docker-compose.dev.yml looks good."
echo "If the SSH test passed, try running: docker-compose -f docker-compose.dev.yml up"
echo ""
echo "If the SSH connection failed, check:"
echo "- SSH keys are properly set up"
echo "- Remote server is accessible"
echo "- SSH alias is correctly defined in ~/.ssh/config"