import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Replace these with your PostgreSQL credentials
db_username = 'postgres'
db_password = 'ai4m2024'
db_host = 'localhost'
db_port = '5432'
db_name = 'postgres'

# Define the table name
table_name = 'plc_data'

# Define the CSV file path
csv_file_path = '/home/suraj-rathod/Desktop/HUL/CSVs/plc_data_20240630141513_part13.csv'

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file_path)

# Function to create plc_timestamp column
def create_plc_timestamp(row):
    # Convert individual time components to datetime object
    dt = datetime(
        int(row['year']), int(row['month']), int(row['day']),
        int(row['hour']), int(row['minute']), int(row['second']),
        int(row['microseconds'])
    )
    return dt

# Add plc_timestamp column to DataFrame
df['plc_timestamp'] = df.apply(create_plc_timestamp, axis=1)

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname=db_name,
    user=db_username,
    password=db_password,
    host=db_host,
    port=db_port
)
cursor = conn.cursor()

# Iterate through rows and insert data into the table
for index, row in df.iterrows():
    insert_query = sql.SQL("""
    INSERT INTO {table} (
        timestamp, cycle_start, year, month, day, hour, minute, second, microseconds,
        sl21_hoz_sealer_front_1_temperature, sl21_hoz_sealer_rear_1_temperature,
        sl21_hor_sealer_pressure, sl21_hoz_sealing_time, sl21_hoz_sealer_strock_1,
        sl21_hoz_sealer_strock_2, sl21_ver_sealer_strock_1, sl21_ver_sealer_strock_2,
        sl21_hoz_sealer_servo_current, sl21_ver_sealer_servo_current, sl21_sealing_jaw_area_temp,
        sl21_ver_sealer_pressure, sl21_hoz_sealer_servo_temperature, sl21_ver_sealer_servo_temperature,
        sl21_laminate_cof_value, sl_21_batch_cut_on_degree, sl_21_batch_cut_off_degree,
        sl21_ver_sealer_front_1_temp, sl21_ver_sealer_front_2_temp, sl21_ver_sealer_front_3_temp,
        sl21_ver_sealer_front_4_temp, sl21_ver_sealer_front_5_temp, sl21_ver_sealer_front_6_temp,
        sl21_ver_sealer_front_7_temp, sl21_ver_sealer_front_8_temp, sl21_ver_sealer_front_9_temp,
        sl21_ver_sealer_front_10_temp, sl21_ver_sealer_front_11_temp, sl21_ver_sealer_front_12_temp,
        sl21_ver_sealer_front_13_temp, sl21_ver_sealer_rear_1_temp, sl21_ver_sealer_rear_2_temp,
        sl21_ver_sealer_rear_3_temp, sl21_ver_sealer_rear_4_temp, sl21_ver_sealer_rear_5_temp,
        sl21_ver_sealer_rear_6_temp, sl21_ver_sealer_rear_7_temp, sl21_ver_sealer_rear_8_temp,
        sl21_ver_sealer_rear_9_temp, sl21_ver_sealer_rear_10_temp, sl21_ver_sealer_rear_11_temp,
        sl21_ver_sealer_rear_12_temp, sl21_ver_sealer_rear_13_temp, sl21_hopper_level,
        sl21_piston_stroke_length, sl21_laminate_gsm, sl21_seal_init_temp, sl21_coeff_friction,
        sl21_seal_strength, sl21_pulling_servo_motor_current, spare1, spare2, spare3, spare4,
        spare5, spare6, spare7, spare8, spare9, spare10, spare11, spare12, spare13, spare14, spare15,
        cycle_time_ms, plc_timestamp
    ) VALUES (
        %(timestamp)s, %(cycle_start)s, %(year)s, %(month)s, %(day)s, %(hour)s, %(minute)s, %(second)s, %(microseconds)s,
        %(sl21_hoz_sealer_front_1_temperature)s, %(sl21_hoz_sealer_rear_1_temperature)s,
        %(sl21_hor_sealer_pressure)s, %(sl21_hoz_sealing_time)s, %(sl21_hoz_sealer_strock_1)s,
        %(sl21_hoz_sealer_strock_2)s, %(sl21_ver_sealer_strock_1)s, %(sl21_ver_sealer_strock_2)s,
        %(sl21_hoz_sealer_servo_current)s, %(sl21_ver_sealer_servo_current)s, %(sl21_sealing_jaw_area_temp)s,
        %(sl21_ver_sealer_pressure)s, %(sl21_hoz_sealer_servo_temperature)s, %(sl21_ver_sealer_servo_temperature)s,
        %(sl21_laminate_cof_value)s, %(sl_21_batch_cut_on_degree)s, %(sl_21_batch_cut_off_degree)s,
        %(sl21_ver_sealer_front_1_temp)s, %(sl21_ver_sealer_front_2_temp)s, %(sl21_ver_sealer_front_3_temp)s,
        %(sl21_ver_sealer_front_4_temp)s, %(sl21_ver_sealer_front_5_temp)s, %(sl21_ver_sealer_front_6_temp)s,
        %(sl21_ver_sealer_front_7_temp)s, %(sl21_ver_sealer_front_8_temp)s, %(sl21_ver_sealer_front_9_temp)s,
        %(sl21_ver_sealer_front_10_temp)s, %(sl21_ver_sealer_front_11_temp)s, %(sl21_ver_sealer_front_12_temp)s,
        %(sl21_ver_sealer_front_13_temp)s, %(sl21_ver_sealer_rear_1_temp)s, %(sl21_ver_sealer_rear_2_temp)s,
        %(sl21_ver_sealer_rear_3_temp)s, %(sl21_ver_sealer_rear_4_temp)s, %(sl21_ver_sealer_rear_5_temp)s,
        %(sl21_ver_sealer_rear_6_temp)s, %(sl21_ver_sealer_rear_7_temp)s, %(sl21_ver_sealer_rear_8_temp)s,
        %(sl21_ver_sealer_rear_9_temp)s, %(sl21_ver_sealer_rear_10_temp)s, %(sl21_ver_sealer_rear_11_temp)s,
        %(sl21_ver_sealer_rear_12_temp)s, %(sl21_ver_sealer_rear_13_temp)s, %(sl21_hopper_level)s,
        %(sl21_piston_stroke_length)s, %(sl21_laminate_gsm)s, %(sl21_seal_init_temp)s, %(sl21_coeff_friction)s,
        %(sl21_seal_strength)s, %(sl21_pulling_servo_motor_current)s, %(spare1)s, %(spare2)s, %(spare3)s, %(spare4)s,
        %(spare5)s, %(spare6)s, %(spare7)s, %(spare8)s, %(spare9)s, %(spare10)s,
        %(spare11)s, %(spare12)s, %(spare13)s, %(spare14)s, %(spare15)s, %(cycle_time_ms)s, %(plc_timestamp)s
    )
    """).format(table=sql.Identifier(table_name))
    
    # Use the datetime object directly for plc_timestamp
    plc_timestamp = row['plc_timestamp']

    cursor.execute(insert_query, {
        'timestamp': row['timestamp'], 'cycle_start': row['cycle_start'], 'year': row['year'], 'month': row['month'],
        'day': row['day'], 'hour': row['hour'], 'minute': row['minute'], 'second': row['second'], 'microseconds': row['microseconds'],
        'sl21_hoz_sealer_front_1_temperature': row['sl21_hoz_sealer_front_1_temperature'], 'sl21_hoz_sealer_rear_1_temperature': row['sl21_hoz_sealer_rear_1_temperature'],
        'sl21_hor_sealer_pressure': row['sl21_hor_sealer_pressure'], 'sl21_hoz_sealing_time': row['sl21_hoz_sealing_time'], 'sl21_hoz_sealer_strock_1': row['sl21_hoz_sealer_strock_1'],
        'sl21_hoz_sealer_strock_2': row['sl21_hoz_sealer_strock_2'], 'sl21_ver_sealer_strock_1': row['sl21_ver_sealer_strock_1'], 'sl21_ver_sealer_strock_2': row['sl21_ver_sealer_strock_2'],
        'sl21_hoz_sealer_servo_current': row['sl21_hoz_sealer_servo_current'], 'sl21_ver_sealer_servo_current': row['sl21_ver_sealer_servo_current'], 'sl21_sealing_jaw_area_temp': row['sl21_sealing_jaw_area_temp'],
        'sl21_ver_sealer_pressure': row['sl21_ver_sealer_pressure'], 'sl21_hoz_sealer_servo_temperature': row['sl21_hoz_sealer_servo_temperature'], 'sl21_ver_sealer_servo_temperature': row['sl21_ver_sealer_servo_temperature'],
        'sl21_laminate_cof_value': row['sl21_laminate_cof_value'], 'sl_21_batch_cut_on_degree': row['sl_21_batch_cut_on_degree'], 'sl_21_batch_cut_off_degree': row['sl_21_batch_cut_off_degree'],
        'sl21_ver_sealer_front_1_temp': row['sl21_ver_sealer_front_1_temp'], 'sl21_ver_sealer_front_2_temp': row['sl21_ver_sealer_front_2_temp'], 'sl21_ver_sealer_front_3_temp': row['sl21_ver_sealer_front_3_temp'],
        'sl21_ver_sealer_front_4_temp': row['sl21_ver_sealer_front_4_temp'], 'sl21_ver_sealer_front_5_temp': row['sl21_ver_sealer_front_5_temp'], 'sl21_ver_sealer_front_6_temp': row['sl21_ver_sealer_front_6_temp'],
        'sl21_ver_sealer_front_7_temp': row['sl21_ver_sealer_front_7_temp'], 'sl21_ver_sealer_front_8_temp': row['sl21_ver_sealer_front_8_temp'], 'sl21_ver_sealer_front_9_temp': row['sl21_ver_sealer_front_9_temp'],
        'sl21_ver_sealer_front_10_temp': row['sl21_ver_sealer_front_10_temp'], 'sl21_ver_sealer_front_11_temp': row['sl21_ver_sealer_front_11_temp'], 'sl21_ver_sealer_front_12_temp': row['sl21_ver_sealer_front_12_temp'],
        'sl21_ver_sealer_front_13_temp': row['sl21_ver_sealer_front_13_temp'], 'sl21_ver_sealer_rear_1_temp': row['sl21_ver_sealer_rear_1_temp'], 'sl21_ver_sealer_rear_2_temp': row['sl21_ver_sealer_rear_2_temp'],
        'sl21_ver_sealer_rear_3_temp': row['sl21_ver_sealer_rear_3_temp'], 'sl21_ver_sealer_rear_4_temp': row['sl21_ver_sealer_rear_4_temp'], 'sl21_ver_sealer_rear_5_temp': row['sl21_ver_sealer_rear_5_temp'],
        'sl21_ver_sealer_rear_6_temp': row['sl21_ver_sealer_rear_6_temp'], 'sl21_ver_sealer_rear_7_temp': row['sl21_ver_sealer_rear_7_temp'], 'sl21_ver_sealer_rear_8_temp': row['sl21_ver_sealer_rear_8_temp'],
        'sl21_ver_sealer_rear_9_temp': row['sl21_ver_sealer_rear_9_temp'], 'sl21_ver_sealer_rear_10_temp': row['sl21_ver_sealer_rear_10_temp'], 'sl21_ver_sealer_rear_11_temp': row['sl21_ver_sealer_rear_11_temp'],
        'sl21_ver_sealer_rear_12_temp': row['sl21_ver_sealer_rear_12_temp'], 'sl21_ver_sealer_rear_13_temp': row['sl21_ver_sealer_rear_13_temp'], 'sl21_hopper_level': row['sl21_hopper_level'],
        'sl21_piston_stroke_length': row['sl21_piston_stroke_length'], 'sl21_laminate_gsm': row['sl21_laminate_gsm'], 'sl21_seal_init_temp': row['sl21_seal_init_temp'], 'sl21_coeff_friction': row['sl21_coeff_friction'],
        'sl21_seal_strength': row['sl21_seal_strength'], 'sl21_pulling_servo_motor_current': row['sl21_pulling_servo_motor_current'], 'spare1': row['spare1'], 'spare2': row['spare2'], 'spare3': row['spare3'], 'spare4': row['spare4'],
        'spare5': row['spare5'], 'spare6': row['spare6'], 'spare7': row['spare7'], 'spare8': row['spare8'], 'spare9': row['spare9'], 'spare10': row['spare10'],
        'spare11': row['spare11'], 'spare12': row['spare12'], 'spare13': row['spare13'], 'spare14': row['spare14'], 'spare15': row['spare15'], 'cycle_time_ms': row['cycle_time_ms'], 'plc_timestamp': plc_timestamp
    })

    conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
