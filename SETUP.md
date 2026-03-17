# Setup

I used PostgreSQL.

## 1. Activate virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 2. Install requirements

```bash
pip install -r requirements.txt
```

## 3. Create `.env`

Create a `.env` file in the project root and add:

```env
DB_HOST=your_host
DB_PORT=your_port
DB_NAME=crm_dwh_test
DB_USER=your_user
DB_PASSWORD=your_password
```

## 4. Create the database

```bash
createdb crm_dwh_test
```

## 5. Verify `.env`

Make sure the database name matches:

```env
DB_NAME=crm_dwh_test
```

## 6. Run the script to create row tables

```bash
python test_data_create.py
```

Optional:

## Restore my database
A full portable PostgreSQL dump is provided in `full_dump.dump`.

```bash
createdb my_test_db
```

```bash
pg_restore -d my_test_db data/full_dump.dump
```