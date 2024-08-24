import pandas as pd
from hdfs import InsecureClient

def store_data_in_hdfs(transaction_data):
    #['353', 'iPhone 14 128GB Chính Hãng VN/A', 'Apple', '128.0', '6.0', 
    # 'Apple A15 Bionic (5 nm)', '6.1', '12 MP, f/2.4, 13mm, 120˚ (góc siêu rộng)12 MP f/1.5, 
    # 26mm (góc rộng)', '3279.0', '21989000.0', '12', '12', '0', '2']

    columns = ['Id', 'Tên sản phẩm', 'Hãng sản xuất', 'Rom', 'Ram', 
               'Chipset(CPU)', 'Kích thước màn hình', 'Độ phân giải Camera', 
               'Dung lượng pin', 'Giá', 'Primary', 'Ultra_Wide', 'Telephoto', 'Hãng sản xuất_']
    transaction_df = pd.DataFrame([transaction_data], columns=columns)

    hdfs_host = 'localhost'
    hdfs_port = 9870

    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')

    # Kiểm tra sự tồn tại của tệp
    try:
        client.status('/batch-layer/raw_data.csv')
        file_exists = True
    except:
        file_exists = False
        
    # if not client.content('/batch-layer/raw_data.csv'):
    #     transaction_df.to_csv('/batch-layer/raw_data.csv', index=False, header=True)
    if not file_exists:
        with client.write('/batch-layer/raw_data.csv', overwrite=True) as writer:
            transaction_df.to_csv(writer, index=False, header=True)

    else:
        with client.read('/batch-layer/raw_data.csv') as reader:
            existing_df = pd.read_csv(reader)
        combined_df = pd.concat([existing_df, transaction_df], ignore_index=True)
        with client.write('/batch-layer/raw_data.csv', overwrite=True) as writer:
            combined_df.to_csv(writer, index=False, header=True)

# if __name__ == "__main__":
#     transaction_data = [1, 'Samsung', 128, 8, 'Exynos 990', 6.7, 4500, 108, 12, 48, 999]

#     # Gọi hàm để lưu dữ liệu
#     store_data_in_hdfs(transaction_data)