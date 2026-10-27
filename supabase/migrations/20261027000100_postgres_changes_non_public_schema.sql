-- Add non-public schema for schema scoping tests
CREATE SCHEMA IF NOT EXISTS cdc_test;

CREATE TABLE IF NOT EXISTS cdc_test.widgets (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text,
  created_at timestamptz DEFAULT now()
);

ALTER TABLE cdc_test.widgets ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Allow all for authenticated" ON cdc_test.widgets;
CREATE POLICY "Allow all for authenticated" ON cdc_test.widgets
  FOR ALL TO authenticated USING (true) WITH CHECK (true);

GRANT USAGE ON SCHEMA cdc_test TO anon, authenticated, service_role;
GRANT ALL ON TABLE cdc_test.widgets TO anon, authenticated, service_role;

-- Add to realtime publication
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_publication WHERE pubname = 'supabase_realtime'
  ) THEN
    CREATE PUBLICATION supabase_realtime;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_publication_tables
    WHERE pubname = 'supabase_realtime' AND schemaname = 'cdc_test' AND tablename = 'widgets'
  ) THEN
    ALTER PUBLICATION supabase_realtime ADD TABLE cdc_test.widgets;
  END IF;
END $$;
