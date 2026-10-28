CREATE TABLE dbo.customers (
    customer_id     INT           NOT NULL PRIMARY KEY,
    document_number NVARCHAR(MAX) NULL,  -- intentionally unbounded: cannot be indexed
    first_name      NVARCHAR(100) NOT NULL,
    last_name       NVARCHAR(100) NOT NULL,
    email           NVARCHAR(255) NULL,
    phone           NVARCHAR(50)  NULL,
    date_of_birth   DATE          NULL,
    created_at      DATETIME2(6)  NOT NULL,
    updated_at      DATETIME2(6)  NOT NULL
);

CREATE TABLE dbo.advances (
    advance_id  INT           NOT NULL PRIMARY KEY,
    customer_id INT           NOT NULL REFERENCES dbo.customers (customer_id),
    amount      DECIMAL(12,2) NOT NULL,
    status      NVARCHAR(20)  NOT NULL,
    created_at  DATETIME2(6)  NOT NULL,
    updated_at  DATETIME2(6)  NOT NULL
);

CREATE TABLE dbo.cards (
    card_id     INT          NOT NULL PRIMARY KEY,
    customer_id INT          NOT NULL REFERENCES dbo.customers (customer_id),
    last_four   NVARCHAR(4)  NOT NULL,
    brand       NVARCHAR(20) NOT NULL,
    status      NVARCHAR(20) NOT NULL,
    created_at  DATETIME2(6) NOT NULL,
    updated_at  DATETIME2(6) NOT NULL
);

CREATE TABLE dbo.transactions (
    transaction_id BIGINT        NOT NULL PRIMARY KEY,
    customer_id    INT           NOT NULL REFERENCES dbo.customers (customer_id),
    card_id        INT           NULL REFERENCES dbo.cards (card_id),
    amount         DECIMAL(12,2) NOT NULL,
    occurred_at    DATETIME2(6)  NOT NULL,
    created_at     DATETIME2(6)  NOT NULL,
    updated_at     DATETIME2(6)  NOT NULL
);

-- Append-only log; no foreign key, so history outlives deleted customers.
CREATE TABLE dbo.customer_history (
    history_id    BIGINT        NOT NULL PRIMARY KEY,
    customer_id   INT           NOT NULL,
    changed_at    DATETIME2(6)  NOT NULL,
    snapshot_json NVARCHAR(MAX) NOT NULL
);

-- Orphaned scratch table: nothing references it and the pipeline does not load it.
CREATE TABLE dbo.tmp_import_2023 (
    import_row  INT           NOT NULL PRIMARY KEY,
    legacy_ref  NVARCHAR(50)  NULL,
    payload     NVARCHAR(MAX) NULL,
    imported_at DATETIME2(6)  NULL
);

-- Test metadata: expected resolution per seeded case. Read by pytest, never loaded.
CREATE TABLE dbo.seed_manifest (
    customer_id          INT          NOT NULL PRIMARY KEY,
    case_name            NVARCHAR(50) NOT NULL,
    expected_status      NVARCHAR(20) NOT NULL,
    expected_survivor_id INT          NULL
);
