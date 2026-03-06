-- Prompt Explorer UI: Database Tables
-- This migration adds tables for tracking LLM calls, prompt sections, budgets, and API keys
-- Run this migration on each agent's GraphJin database

-- ─────────────────────────────────────────────────────────────────
-- Table: llm_calls
-- Stores every LLM call with full metadata, tokens, cost, and status
-- ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS llm_calls (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Identity
    agent_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    conversation_turn INTEGER NOT NULL DEFAULT 1,

    -- Model & Provider
    model VARCHAR(255) NOT NULL,
    provider VARCHAR(100),
    temperature FLOAT,

    -- Prompt Structure
    system_prompt TEXT,
    user_message TEXT,
    enriched_message TEXT,
    full_messages_json JSONB,

    -- Section Breakdown (stored as JSONB for flexibility)
    prompt_sections JSONB,

    -- Tokens & Cost
    input_tokens INTEGER,
    output_tokens INTEGER,
    total_tokens INTEGER,
    estimated_cost_usd NUMERIC(10, 6),

    -- Timing
    created_at TIMESTAMP DEFAULT NOW(),
    latency_ms INTEGER,

    -- Status & Control
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    block_reason VARCHAR(255),

    -- Response
    output TEXT,
    error_message TEXT,

    -- Tool Calls (if any)
    tool_calls_json JSONB,

    -- Metadata
    channel VARCHAR(100),
    sender_id VARCHAR(255)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_llm_calls_agent_session ON llm_calls(agent_id, session_id);
CREATE INDEX IF NOT EXISTS idx_llm_calls_created_at ON llm_calls(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_llm_calls_status ON llm_calls(status);
CREATE INDEX IF NOT EXISTS idx_llm_calls_model ON llm_calls(model);

-- ─────────────────────────────────────────────────────────────────
-- Table: prompt_sections
-- Stores individual sections of each prompt for granular analysis
-- ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS prompt_sections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    llm_call_id UUID NOT NULL REFERENCES llm_calls(id) ON DELETE CASCADE,

    section_name VARCHAR(100) NOT NULL,
    section_order INTEGER NOT NULL,
    content TEXT NOT NULL,
    char_count INTEGER,
    estimated_tokens INTEGER,

    -- Human Feedback for DSPy Optimization
    quality_rating INTEGER CHECK (quality_rating >= 1 AND quality_rating <= 5),
    comments TEXT,
    suggestions TEXT,

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_prompt_sections_call ON prompt_sections(llm_call_id);
CREATE INDEX IF NOT EXISTS idx_prompt_sections_name ON prompt_sections(section_name);

-- ─────────────────────────────────────────────────────────────────
-- Table: prompt_budgets
-- Per-agent budget configuration for token/cost limits
-- ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS prompt_budgets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id VARCHAR(255) NOT NULL UNIQUE,

    -- Token Limits
    max_input_tokens INTEGER,
    max_total_tokens INTEGER,

    -- Cost Limits
    max_cost_per_call_usd NUMERIC(10, 6),
    daily_cost_limit_usd NUMERIC(10, 2),
    monthly_cost_limit_usd NUMERIC(10, 2),

    -- Rate Limiting
    max_calls_per_minute INTEGER,
    max_calls_per_hour INTEGER,

    -- Active/Inactive
    enabled BOOLEAN DEFAULT true,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_prompt_budgets_agent ON prompt_budgets(agent_id);

-- ─────────────────────────────────────────────────────────────────
-- Table: llmrouter_api_keys
-- Manages LLM Router API keys per agent
-- ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS llmrouter_api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id VARCHAR(255) NOT NULL,

    -- Key Info (never store plaintext keys!)
    api_key_hash VARCHAR(255) NOT NULL,
    api_key_prefix VARCHAR(20) NOT NULL,
    llmrouter_key_id VARCHAR(255) NOT NULL,

    -- Metadata
    name VARCHAR(255),
    description TEXT,

    -- Status
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    expires_at TIMESTAMP,

    -- Usage Tracking
    total_calls INTEGER DEFAULT 0,
    total_tokens INTEGER DEFAULT 0,
    total_cost_usd NUMERIC(10, 2) DEFAULT 0,

    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_llmrouter_keys_agent ON llmrouter_api_keys(agent_id);
CREATE INDEX IF NOT EXISTS idx_llmrouter_keys_status ON llmrouter_api_keys(status);

-- ─────────────────────────────────────────────────────────────────
-- Comments for documentation
-- ─────────────────────────────────────────────────────────────────

COMMENT ON TABLE llm_calls IS 'Tracks every LLM call with full prompt, response, and metadata';
COMMENT ON TABLE prompt_sections IS 'Stores prompt sections separately for granular analysis and DSPy optimization';
COMMENT ON TABLE prompt_budgets IS 'Per-agent budget configuration for cost and token control';
COMMENT ON TABLE llmrouter_api_keys IS 'Manages LLM Router API keys with usage tracking';

COMMENT ON COLUMN llm_calls.status IS 'pending, allowed, blocked, completed, failed';
COMMENT ON COLUMN llm_calls.prompt_sections IS 'JSONB map of section_name -> content for quick access';
COMMENT ON COLUMN prompt_sections.quality_rating IS 'Human rating 1-5 for DSPy optimization';

-- ─────────────────────────────────────────────────────────────────
-- Row Level Security (RLS) Policies
-- ─────────────────────────────────────────────────────────────────

-- Enable RLS on all tables
ALTER TABLE llm_calls ENABLE ROW LEVEL SECURITY;
ALTER TABLE prompt_sections ENABLE ROW LEVEL SECURITY;
ALTER TABLE prompt_budgets ENABLE ROW LEVEL SECURITY;
ALTER TABLE llmrouter_api_keys ENABLE ROW LEVEL SECURITY;

-- Policy: Allow GraphJin service role full access to all tables
-- Note: Replace 'service_role' with your actual service role if different
CREATE POLICY "GraphJin service can manage llm_calls"
    ON llm_calls
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

CREATE POLICY "GraphJin service can manage prompt_sections"
    ON prompt_sections
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

CREATE POLICY "GraphJin service can manage prompt_budgets"
    ON prompt_budgets
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

CREATE POLICY "GraphJin service can manage llmrouter_api_keys"
    ON llmrouter_api_keys
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- Policy: Allow authenticated users to read their own agent's data
CREATE POLICY "Users can read their agent's llm_calls"
    ON llm_calls
    FOR SELECT
    TO authenticated
    USING (
        agent_id IN (
            -- Replace with your actual logic to map auth.uid() to agent_id
            -- For example, if you have an agents table:
            -- SELECT id FROM agents WHERE user_id = auth.uid()
            -- For now, allowing all authenticated users to read
            SELECT agent_id FROM llm_calls
        )
    );

CREATE POLICY "Users can read their agent's prompt_sections"
    ON prompt_sections
    FOR SELECT
    TO authenticated
    USING (
        llm_call_id IN (
            SELECT id FROM llm_calls
            -- Add WHERE clause to filter by user's agents
        )
    );

CREATE POLICY "Users can read their agent's budgets"
    ON prompt_budgets
    FOR SELECT
    TO authenticated
    USING (
        agent_id IN (
            -- Map to user's agents
            SELECT agent_id FROM prompt_budgets
        )
    );

CREATE POLICY "Users can read their agent's api_keys"
    ON llmrouter_api_keys
    FOR SELECT
    TO authenticated
    USING (
        agent_id IN (
            -- Map to user's agents
            SELECT agent_id FROM llmrouter_api_keys
        )
    );

-- ─────────────────────────────────────────────────────────────────
-- GRANTS for Service Role
-- ─────────────────────────────────────────────────────────────────

-- Grant all privileges to service_role (used by GraphJin)
GRANT ALL PRIVILEGES ON TABLE llm_calls TO service_role;
GRANT ALL PRIVILEGES ON TABLE prompt_sections TO service_role;
GRANT ALL PRIVILEGES ON TABLE prompt_budgets TO service_role;
GRANT ALL PRIVILEGES ON TABLE llmrouter_api_keys TO service_role;

-- Grant sequence usage for UUID generation
GRANT USAGE ON SCHEMA public TO service_role;

-- Grant read access to authenticated users (webapp queries)
GRANT SELECT ON TABLE llm_calls TO authenticated;
GRANT SELECT ON TABLE prompt_sections TO authenticated;
GRANT SELECT ON TABLE prompt_budgets TO authenticated;
GRANT SELECT ON TABLE llmrouter_api_keys TO authenticated;

-- ─────────────────────────────────────────────────────────────────
-- Notes for Supabase Deployment
-- ─────────────────────────────────────────────────────────────────

-- 1. GraphJin should use the service_role key to bypass RLS
-- 2. Webapp/Backend should use anon/authenticated keys with RLS enabled
-- 3. Update RLS policies to match your actual user->agent relationship
-- 4. Consider adding policies for users to update quality_rating in prompt_sections
-- 5. Add audit logging if needed (Supabase has built-in audit log triggers)
