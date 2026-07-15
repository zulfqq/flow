BEGIN;

-- Whether a data plane is enabled and available for use. Disabled data planes
-- remain in the catalog (and keep serving existing tasks) but are hidden from
-- the default `dataPlanes` listing, so operators can retire a plane from new
-- selection without deleting its record. Defaults to true so every existing
-- plane stays enabled.
ALTER TABLE public.data_planes
ADD COLUMN enabled boolean NOT NULL DEFAULT true;

GRANT
SELECT
  (enabled) ON public.data_planes TO authenticated;

COMMENT ON COLUMN public.data_planes.enabled IS 'Whether this data plane is enabled and available for use';

COMMIT;
