from datetime import datetime
import happybase



def insert_dataHbase(data):
    connection = happybase.Connection('localhost')
    connection.open()

    if b'smartphone' not in connection.tables():
        # Create a table 'smartphone' with column families
        connection.create_table(
            'smartphone',
            {
                'info': dict()  # Column family 'info'
            }
        )


    table = connection.table('smartphone')


    # Generate a unique row key based on current timestamp
    row_key = datetime.now().strftime('%Y%m%d%H%M%S%f')

    data_to_insert = {
        b'info:date': bytes(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'utf-8'),
        b'info:Brand': bytes(data[0], 'utf-8'),
        b'info:Screen_size': bytes(str(data[3]), 'utf-8'),
        b'info:RAM': bytes(str(data[2]), 'utf-8'),
        b'info:Storage': bytes(str(data[1]), 'utf-8'),
        b'info:Battery': bytes(str(data[4]), 'utf-8'),
        b'info:Cam_Prim': bytes(str(data[5]), 'utf-8'),
        b'info:Cam_Ultra': bytes(str(data[6]), 'utf-8'),
        b'info:Cam_Tele': bytes(str(data[7]), 'utf-8'),
        b'info:Price': bytes(str(data[8]), 'utf-8')
    }

    table.put(bytes(row_key, 'utf-8'), data_to_insert)

    print("Data inserted into HBase: ", data)


    connection.close()

# sample_data = ["Samsung", 128, 8, 6.1, 4000, 12, 48, 8, 29000000]
# insert_dataHbase(sample_data)