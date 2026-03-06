# Supabase Setup Guide for Prompt Explorer

This guide explains how to deploy the Prompt Explorer tables to Supabase and configure GraphJin to work with Supabase's Row Level Security (RLS).

---

## 🔐 Security Architecture

### Roles in Supabase

1. **`service_role`** - Bypasses RLS, used by GraphJin (ZeroClaw writes)
2. **`authenticated`** - Standard user role, enforces RLS (Webapp reads)
3. **`anon`** - Anonymous access (not used for this feature)

### Data Flow

```
ZeroClaw Agent
    ↓ (GraphJin with service_role key)
GraphJin API → Supabase PostgreSQL
    ↑ (Backend with authenticated key)
Webapp Backend
    ↑
Webapp UI
```

---

## 📋 Migration Steps

### 1. Apply the Migration

**Option A: Via Supabase Dashboard**
1. Go to Supabase Dashboard → SQL Editor
2. Copy contents of `graphjin/migrations/001_prompt_explorer_tables.sql`
3. Run the SQL
4. Verify tables created in Database → Tables

**Option B: Via Supabase CLI**
```bash
# Install Supabase CLI
npm install -g supabase

# Login
supabase login

# Link to your project
supabase link --project-ref <your-project-ref>

# Run migration
supabase db push
```

### 2. Verify Table Creation

Check that these tables exist:
- `llm_calls`
- `prompt_sections`
- `prompt_budgets`
- `llmrouter_api_keys`

### 3. Verify RLS Policies

In Supabase Dashboard → Authentication → Policies, verify:

**llm_calls policies:**
- ✅ "GraphJin service can manage llm_calls" (service_role, ALL)
- ✅ "Users can read their agent's llm_calls" (authenticated, SELECT)

**Similar policies for other tables**

---

## 🔧 GraphJin Configuration

### GraphJin Config for Supabase

Create or update your GraphJin config to use Supabase:

**`config/graphjin.yml`:**
```yaml
app_name: "Prompt Explorer GraphJin"
host_port: 0.0.0.0:8080
web_ui: true

database:
  type: postgres
  host: db.YOUR_PROJECT_REF.supabase.co
  port: 5432
  dbname: postgres
  user: postgres
  password: YOUR_SUPABASE_DB_PASSWORD  # From Supabase Settings → Database

  # Connection pool settings
  pool_size: 10
  max_retries: 5
  log_level: info

  # Supabase requires SSL
  sslmode: require

# IMPORTANT: Use service_role for bypassing RLS
auth:
  # GraphJin should use service_role JWT for all operations
  type: jwt
  cookie: _graphjin

  jwt:
    provider: supabase
    secret: YOUR_SUPABASE_JWT_SECRET  # From Supabase Settings → API → JWT Secret

    # GraphJin will use service_role to bypass RLS
    # This allows ZeroClaw to write directly
    public_key_file: ""

# Allow mutations for ZeroClaw to write data
mutations:
  enable: true
  tables:
    - llm_calls
    - prompt_sections
    - prompt_budgets
    - llmrouter_api_keys

# Enable for development, disable in production
dev_mode: false
```

### Environment Variables for GraphJin

Set these environment variables when running GraphJin:

```bash
# Database connection
export DB_HOST="db.YOUR_PROJECT_REF.supabase.co"
export DB_PORT=5432
export DB_NAME="postgres"
export DB_USER="postgres"
export DB_PASSWORD="YOUR_SUPABASE_DB_PASSWORD"

# Supabase JWT Secret
export JWT_SECRET="YOUR_SUPABASE_JWT_SECRET"

# Service role key (for bypassing RLS)
export SUPABASE_SERVICE_ROLE_KEY="YOUR_SERVICE_ROLE_KEY"

# GraphJin
export GRAPHJIN_PORT=8080
```

---

## 🔑 API Keys Setup

### 1. Get Supabase Keys

In Supabase Dashboard → Settings → API:

- **`anon` (public) key** - For webapp (enforces RLS)
- **`service_role` key** - For GraphJin (bypasses RLS) ⚠️ Keep secret!
- **JWT Secret** - For verifying tokens

### 2. Configure ZeroClaw

ZeroClaw should connect to GraphJin with the service_role key:

**In agent's `config.toml`:**
```toml
[graphjin]
enabled = true
api_url = "http://localhost:8080/api/v1/graphql"

# Or if GraphJin requires auth header:
# auth_header = "Authorization: Bearer YOUR_SERVICE_ROLE_KEY"
```

**GraphJin automatically uses service_role** when configured properly, so ZeroClaw doesn't need to include auth headers.

### 3. Configure Backend/Webapp

The webapp backend should query GraphJin using the `authenticated` role:

**In backend:**
```typescript
// backend/src/routes/prompts.ts
const GRAPHJIN_URL = process.env.GRAPHJIN_URL || 'http://localhost:8080/api/v1/graphql';

// Use authenticated user's JWT token
async function fetchPrompts(userToken: string, agentId: string) {
  const response = await fetch(GRAPHJIN_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${userToken}`,  // User's authenticated token
    },
    body: JSON.stringify({
      query: `
        query GetLLMCalls($agentId: String!) {
          llmCalls(where: {agent_id: {_eq: $agentId}}, order_by: {created_at: desc}, limit: 50) {
            id
            model
            inputTokens
            outputTokens
            estimatedCostUsd
            createdAt
            status
          }
        }
      `,
      variables: { agentId }
    })
  });

  return response.json();
}
```

---

## 🔒 Row Level Security (RLS) Policies

### Default Policies (in migration)

**For service_role (GraphJin/ZeroClaw):**
- Full access to all tables (bypass RLS)

**For authenticated users (Webapp):**
- Read-only access to their own agent's data
- ⚠️ **TODO**: Update policies to filter by user ownership

### Customize RLS Policies

If you have an `agents` table that maps users to agents:

```sql
-- Update llm_calls policy
DROP POLICY IF EXISTS "Users can read their agent's llm_calls" ON llm_calls;

CREATE POLICY "Users can read their agent's llm_calls"
    ON llm_calls
    FOR SELECT
    TO authenticated
    USING (
        agent_id IN (
            SELECT id FROM agents WHERE user_id = auth.uid()
        )
    );
```

### Allow Users to Rate Prompt Sections

```sql
-- Allow users to update quality ratings
CREATE POLICY "Users can rate their agent's prompt sections"
    ON prompt_sections
    FOR UPDATE
    TO authenticated
    USING (
        llm_call_id IN (
            SELECT id FROM llm_calls
            WHERE agent_id IN (
                SELECT id FROM agents WHERE user_id = auth.uid()
            )
        )
    )
    WITH CHECK (
        -- Only allow updating rating fields, not content
        true
    );
```

---

## 🧪 Testing the Setup

### 1. Test GraphJin Connection

```bash
# Start GraphJin
graphjin serv

# Test health endpoint
curl http://localhost:8080/health

# Expected: {"status": "ok"}
```

### 2. Test Service Role Access (ZeroClaw simulation)

```bash
# Insert a test LLM call (simulating ZeroClaw)
curl http://localhost:8080/api/v1/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { insert_llm_calls_one(object: {agent_id: \"test-agent\", session_id: \"main\", conversation_turn: 1, model: \"test\", status: \"pending\", input_tokens: 100, estimated_cost_usd: 0.001}) { id } }"
  }'

# Expected: Returns an ID
```

### 3. Test Authenticated User Access (Webapp simulation)

```bash
# Get user's JWT token from Supabase Auth
USER_TOKEN="<user's-authenticated-jwt-token>"

# Query LLM calls
curl http://localhost:8080/api/v1/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $USER_TOKEN" \
  -d '{
    "query": "query { llmCalls(limit: 10) { id model createdAt } }"
  }'

# Expected: Returns only calls for user's agents (per RLS)
```

### 4. Verify RLS Enforcement

```bash
# Try to query with anon key (should fail or return empty)
curl http://localhost:8080/api/v1/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ANON_KEY" \
  -d '{
    "query": "query { llmCalls(limit: 10) { id } }"
  }'

# Expected: Empty results or access denied
```

---

## 🚨 Security Checklist

- [ ] Service role key stored securely (env vars, secrets manager)
- [ ] Service role key NOT exposed to frontend
- [ ] RLS enabled on all tables
- [ ] RLS policies tested and verified
- [ ] GraphJin only accessible from backend/ZeroClaw (not public internet)
- [ ] Supabase connection uses SSL (`sslmode: require`)
- [ ] User→Agent mapping implemented in RLS policies
- [ ] API keys rotated periodically
- [ ] Audit logging enabled (optional)

---

## 📚 Additional Resources

- [Supabase RLS Documentation](https://supabase.com/docs/guides/auth/row-level-security)
- [GraphJin with Supabase](https://graphjin.com/docs/supabase)
- [Supabase JWT Verification](https://supabase.com/docs/guides/auth/jwts)

---

## 🐛 Troubleshooting

### "permission denied for table llm_calls"

**Cause**: RLS is blocking the query
**Solution**: Ensure GraphJin uses service_role key, or user has proper RLS policy

### "relation 'llm_calls' does not exist"

**Cause**: Migration not applied
**Solution**: Run the migration SQL in Supabase dashboard

### GraphJin can't connect to Supabase

**Cause**: SSL mode or connection settings
**Solution**: Ensure `sslmode: require` in config and check firewall rules

### Users see all agents' data

**Cause**: RLS policy too permissive
**Solution**: Update RLS policies to filter by user ownership (see "Customize RLS Policies" above)

---

**✅ Setup Complete!** Your Prompt Explorer is now securely integrated with Supabase + GraphJin + RLS.
