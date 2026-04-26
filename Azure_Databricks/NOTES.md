
Orchestration with Synapse Serverless Only
        Serverless-Only Flow
                Blob Storage holds raw → cleaned → dim → fact parquet.
                Synapse Serverless external tables/views expose parquet as relational schema.
                Queries + Views replace ADF Data Flows.
                Power BI connects directly to Synapse views for reporting.

        Final Flow (All in Synapse GUI)
                Data tab → Connect Blob + preview files.
                Develop tab → Save SQL scripts + create views.
                Integrate tab → Build pipelines (like Airflow DAGs).
                Monitor tab → Track runs.
                Power BI → Connect to Synapse views.

Step 1: Land Data in Blob Storage (This is your data lak)
        Your containers are already set up:
        raw → raw parquet from ETL.
        cleaned → cleaned parquet.
        dim → dimension parquet.
        fact → fact parquet.

Step 2: Connect Blob Storage
        In Synapse Studio → go to Data (left menu).
        Click Linked → + Connect to external data.
        Choose Azure Data Lake Storage Gen2 / Blob Storage.
        Select your storage account → test connection → Create.
        Now you’ll see your containers (raw, cleaned, dim, fact) in the GUI under “Linked”.

Step 3: Explore & Preview Files
        Expand your linked storage → click into raw → you’ll see your parquet files.
        Right-click a parquet file → New SQL Script → Select TOP 100 rows.
        Synapse auto-generates a SELECT statement using OPENROWSET.
                SELECT TOP 100 *
                        FROM OPENROWSET(
                        BULK 'raw/*.parquet',
                        DATA_SOURCE = 'YourStorageLinkedService',
                        FORMAT = 'PARQUET'
                        ) AS rows;

Step 4: Save Queries as SQL Scripts
        After Synapse generates the query, click Save As → SQL Script.
        Name it raw_to_cleaned.sql.
        Repeat for other steps:
        Create one script for cleaned data (with filters, dedup, renaming).
        Create one script for dimensions (driver, payment, reason).
        Create one script for fact table (joining dims with trips).

Step 5: Create Views (Optional)
        Instead of just scripts, you can create views that always point to the latest files.
        From the query window → modify the auto-generated script:
                CREATE VIEW vw_cleaned_trips AS
                SELECT DISTINCT *
                FROM OPENROWSET(
                BULK 'cleaned/*.parquet',
                DATA_SOURCE = 'YourStorageLinkedService',
                FORMAT = 'PARQUET'
                ) AS rows;
        Save → now you can query vw_cleaned_trips anytime.
        Do the same for vw_dim_driver, vw_dim_payment, vw_dim_reason, and vw_fact_trips.

Step 6: Automate with Synapse Pipelines (GUI)
        Synapse Studio has a lightweight version of ADF pipelines:
        In left menu → click Integrate.
        Click + New → Pipeline.
        Add activities:
        SQL script activity → point to your saved scripts (raw_to_cleaned.sql, build_dim_driver.sql, etc.).
        Set dependencies → just drag arrows (RawToCleaned → Dims → Fact).
        Save pipeline as UberMasterPipeline.
        This gives you a visual DAG inside Synapse itself.

Step 7: Schedule Pipeline
        Inside pipeline editor → click Add Trigger → New/Edit.
        Choose Schedule trigger (daily, hourly) OR Event trigger (fires when new parquet lands in Blob).
        Attach trigger to UberMasterPipeline.
        Now Synapse alone handles orchestration.

Step 8: Monitor
        Go to Monitor (left menu).
        You’ll see pipeline runs (Success / Failed), duration, logs.
        Drill into each activity to see query details.

Step 9: Power BI
        In Power BI → Get Data → Azure Synapse Analytics (SQL on-demand).
        Enter your Synapse workspace endpoint.
        Pick your views (e.g., vw_fact_trips).
        Build dashboards.





































🛠 Step 1: Create Azure Data Factory

        Go to Azure Portal
        .

        Search for Data Factory → click Create.

        Choose your existing Resource Group.

        Give it a name like uber-adf-pipeline.(uk south)

        Region: choose same region as your storage & Synapse.

        Click Review + Create, then Create.

        After deployment, click Go to Resource.

        This is your control room where we’ll build pipelines (like Airflow DAGs but with a GUI).


🛠 Step 2: Connect ADF to Your Storage

        In Data Factory, open the left menu → Manage (gear icon).

        Click Linked services → New.

        Select Azure Blob Storage.

        Choose Your Storage Account (where you already have raw, cleaned, dim, fact).

        Test connection → Create.

        Now ADF can “see” your Blob storage.


🛠 Step 3: Create Synapse
        Search Synapse Analytics → click Create
        Resource Group → choose your existing one.

        Workspace name → e.g., uber-synapse.

        Region → same as ADF + Blob.

        Data Lake Storage (Gen2) → select your storage account and create/choose a container (e.g., synapse).  (new) https://uberdlstorage.dfs.core.windows.net
                    urberridesbooking
                    (new) uber-synapse
                    uberride@25/ sqladminuser

        Click Review + Create → then Create.

        This gives you a Synapse workspace with a default serverless SQL pool.


    
 🛠 Step C: Connect Synapse to ADF

        In ADF → go to Manage (gear icon) → Linked Services → + New.

        Search Azure Synapse Analytics (SQL on-demand).for dedicated sql pool [azure SQLdatabase for serverless pool]

        Choose your Synapse Workspace (no need for a dedicated pool).

        Authentication: Managed Identity (recommended) or SQL user/password.

        Test connection → Create.


🛠 Step 4: Define Your Datasets

        Think of datasets as pointers to data (files in Blob or queryable objects in Synapse).

        In ADF → Author → Datasets → + New Dataset:

        raw_dataset → points to raw container (parquet).

        cleaned_dataset → points to cleaned container.

        dim_dataset → points to dim container.

        fact_dataset → points to fact container.

        (Optional) If you want ADF to run SQL scripts against Synapse serverless (e.g. create external tables/views), also create Synapse datasets.

        Example: ext_dim_driver, ext_fact_trips (these don’t hold data, they’re query entry points).

        Now ADF knows where to read/write files, and can optionally run SQL statements in Synapse.

🛠 Step 5: Build First Pipeline (Raw → Cleaned)

        In ADF → Pipelines → + New Pipeline.

        Name it RawToCleaned.

        Drag in a Mapping Data Flow activity.

        Inside Mapping Data Flow:

        Source = raw_dataset (parquet files).

        Transformations = remove duplicates, drop nulls, rename columns.

        Sink = cleaned_dataset.

        This is your data quality / cleaning step (replaces Python cleaning).

🛠 Step 6: Build Dimension Pipelines

        For each dimension (driver, payment, reason):

        Create pipeline BuildDimDriver.

        Add a Mapping Data Flow:

        Source = cleaned_dataset.

        Transformation = select distinct driver_id, driver_name.

        Sink = write parquet into dim_dataset/driver/.

        Repeat for:

        BuildDimPayment (select payment_type).

        BuildDimReason (select reason_code, description).

        These are your dimension parquet outputs in Blob.

🛠 Step 7: Build Fact Pipeline

        Create pipeline BuildFactTrips.

        Add a Mapping Data Flow:

        Source = cleaned_dataset.

        Joins = bring in attributes from parquet in dim_dataset (driver, payment, reason).

        Sink = write parquet into fact_dataset/trips/.

         With serverless, this parquet in fact is your fact table — no loading needed.

🛠 Step 8: Orchestration (Control Flow)

        Create a master pipeline → UberMasterPipeline.

        Order activities like a DAG:

        RawToCleaned → runs first.

        BuildDimDriver, BuildDimPayment, BuildDimReason → run in parallel after cleaned step.

        BuildFactTrips → runs last (depends on all dims).

        This ensures fact always waits for dimensions.

🛠 Step 9: Triggers & Scheduling

        Go to Triggers → + New Trigger.

        Choose:

        Schedule trigger (e.g., run daily at midnight).

        Event trigger (fires automatically when a new file lands in raw container).

        Attach the trigger to UberMasterPipeline.

🛠 Step 10: Monitoring

        In ADF → Monitor tab.

        You’ll see each pipeline run → status, duration, errors.

        Drill into failed activities to see logs.

        (Optional) Send alerts → Azure Monitor → email/Teams notifications.

🛠 Step 11: Power BI with Synapse Serverless

        Now connect the parquet data to Power BI via serverless external tables.

        In Synapse Studio → Develop → SQL scripts.

        Register your dim and fact parquet folders as external tables: