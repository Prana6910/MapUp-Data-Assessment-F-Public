import pandas as pd
df=pd.read_csv('C:\\Users\\user\\Desktop\\python\\dataset-2.csv')
def generate_car_matrix(df) -> pd.DataFrame:
    df = df.pivot(index='id_1', columns='id_2', values='car').fillna(0)
    for i in df.index:
        df.at[i, i] = 0
    return df

def get_type_count(df)->dict:
    df['car_type'] = pd.cut(df['car'], bins=[-float('inf'), 15, 25, float('inf')],
                            labels=['low', 'medium', 'high'], right=False)
    dict = df['car_type'].value_counts().sort_index().to_dict()
    return dict


def get_bus_indexes(df) -> list:
    bus_mean = df['bus'].mean()    
    list = df.loc[df['bus'] > 2 * bus_mean].index.tolist()
    return list

def filter_routes(df) -> list:
    route_avg_truck = df.groupby('route')['truck'].mean()  
    list = route_avg_truck[route_avg_truck > 7].index.tolist()
    list.sort()
    return list

def multiply_matrix(matrix) -> pd.DataFrame:
    def custom_multiply(value):
        if value > 20:
            return round(value * 0.75, 1)
        else:
            return round(value * 1.25, 1)   
    matrix = matrix.map(custom_multiply)
    return matrix

def time_check(df)->pd.Series:    
    df['start_timestamp'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'], format='%A %H:%M:%S')    
    df['end_timestamp'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'], format='%A %H:%M:%S')
    df['time_diff'] = df['end_timestamp'] - df['start_timestamp']    
    full_24_hours = df['time_diff'] >= pd.Timedelta(hours=24)    
    span_all_days = (df['start_timestamp'].dt.day_name() == df['end_timestamp'].dt.day_name())    
    result_series = full_24_hours & span_all_days
    result_series = result_series.groupby([df['id'], df['id_2']]).all()
    return result_series
