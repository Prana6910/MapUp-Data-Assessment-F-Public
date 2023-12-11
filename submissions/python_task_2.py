import pandas as pd

def calculate_distance_matrix(df)->pd.DataFrame():
    distance_matrix = pd.DataFrame(index=df['id_start'].unique(), columns=df['id_end'].unique())
    distance_matrix = distance_matrix.fillna(0)

    for index, row in df.iterrows():
        id_start = row['id_start']
        id_end = row['id_end']

        if id_start in distance_matrix.index and id_end in distance_matrix.columns:
            distance_matrix.at[id_start, id_end] += int(row['distance'])

    return distance_matrix


def unroll_distance_matrix(df) -> pd.DataFrame:
    unrolled_df = pd.DataFrame(columns=['id_start', 'id_end', 'distance'])

    for index, row in df.iterrows():
        for col in df.columns:
            if row[col] != 0:
                unrolled_df = pd.concat([unrolled_df, pd.DataFrame({'id_start': [index], 'id_end': [col], 'distance': [row[col]]})], ignore_index=True)

    return unrolled_df

def find_ids_within_ten_percentage_threshold(df, reference_id) -> pd.DataFrame:
    reference_avg_distance = df[df['id_start'] == reference_id]['distance'].mean()
    threshold = 0.1 * reference_avg_distance

    selected_ids = df[(df['id_start'] != reference_id) & (df['distance'] >= reference_avg_distance - threshold) & (df['distance'] <= reference_avg_distance + threshold)]['id_start'].unique()
    
    result_df = pd.DataFrame({'id_start': selected_ids})
    result_df = result_df.sort_values(by='id_start').reset_index(drop=True)

    return result_df

def calculate_toll_rate(df) -> pd.DataFrame:
    toll_rates = {'moto': 0.8, 'car': 1.2, 'rv': 1.5, 'bus': 2.2, 'truck': 3.6}

    for vehicle_type in toll_rates.keys():
        df[vehicle_type] = df['distance'] * toll_rates[vehicle_type]

    return df

def calculate_time_based_toll_rates(df) -> pd.DataFrame:
    time_discounts = {
        'weekday_00-10': 0.8,
        'weekday_10-18': 1.2,
        'weekday_18-24': 0.8,
        'weekend_all_day': 0.7
    }

    daily_dfs = []
    
    for start_day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
        for end_day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
            daily_df = df.copy()  
            daily_df['start_day'] = start_day
            daily_df['end_day'] = end_day

            
            daily_df['start_time'] = pd.to_datetime('00:00:00').time()
            daily_df['end_time'] = pd.to_datetime('23:59:59').time()

            if 'timestamp' in daily_df.columns and pd.api.types.is_datetime64_any_dtype(daily_df['timestamp']):
                daily_df['start_time'] = daily_df['timestamp'].dt.time
                daily_df['end_time'] = daily_df['timestamp'].dt.time

                
                daily_df.loc[(0 <= daily_df['start_time'].dt.hour) & (daily_df['start_time'].dt.hour < 10), 'distance'] *= time_discounts['weekday_00-10']
                daily_df.loc[(10 <= daily_df['start_time'].dt.hour) & (daily_df['start_time'].dt.hour < 18), 'distance'] *= time_discounts['weekday_10-18']
                daily_df.loc[(18 <= daily_df['start_time'].dt.hour), 'distance'] *= time_discounts['weekday_18-24']
                daily_df.loc[(start_day in ['Saturday', 'Sunday']), 'distance'] *= time_discounts['weekend_all_day']

            daily_dfs.append(daily_df)

    result_df = pd.concat(daily_dfs, ignore_index=True)

    result_df=result_df[['id_start', 'id_end', 'distance', 'start_day', 'start_time', 'end_day', 'end_time', 'moto', 'car', 'rv', 'bus', 'truck']]
    return result_df
