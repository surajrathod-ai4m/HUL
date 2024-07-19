import psycopg2
from psycopg2.extras import RealDictCursor

# Database connection parameters
db_params = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'ai4m2024'
}

def calculate_averages(cursor):
    cursor.execute("""
        WITH max_values AS (
            SELECT sl21_hoz_sealer_servo_current
            FROM plc_data
            ORDER BY sl21_hoz_sealer_servo_current DESC
            LIMIT 10000
        ),
        min_values AS (
            SELECT sl21_hoz_sealer_servo_current
            FROM plc_data
            ORDER BY sl21_hoz_sealer_servo_current ASC
            LIMIT 10000
        )
        SELECT 
            AVG(max_avg.avg_max) as avg_max,
            AVG(min_avg.avg_min) as avg_min
        FROM 
            (SELECT AVG(sl21_hoz_sealer_servo_current) as avg_max FROM max_values) as max_avg,
            (SELECT AVG(sl21_hoz_sealer_servo_current) as avg_min FROM min_values) as min_avg
    """)
    return cursor.fetchone()

def main():
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Calculate averages
        result = calculate_averages(cursor)

        # Print results
        print("Column: sl21_hoz_sealer_servo_current")
        print(f"  Average of top 1000 max values: {result['avg_max']}")
        print(f"  Average of top 1000 min values: {result['avg_min']}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()