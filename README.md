# BigData-Project-IT4931
## Giới thiệu bài toán
Dữ liệu được lấy từ trang web nghe nhạc bao gồm: các thông tin về bài hát (tên bài hát, ca sĩ, thời lượng, năm phát hành, ...) và các thông tin truy cập của người dùng. Từ đó thực hiện lưu trữ, xử lý và visualize dữ liệu. Như vậy, từ dữ liệu ban đầu, sau khi được xử lý, phân tích, ta thu được các thông tin như sau: 

- Thống kê các bài hát và hiển thị top các bài hát được nghe nhiều nhất. 

- Xu bướng lượt nghe 5 bài hát được nghe nhiều nhất trong tháng. 

- Biểu đồ lượt truy cập củ 5 người dùng có lượt truy cập nhiều nhất tháng. 

- Top các thành phố tiềm năng nhất trong tháng. 

- Xu hướng tổng lượt truy cập của người dùng ở 5 thành phố tiềm năng nhất tháng. 

## Tools
- Apache Hadoop
- Apache Kafka
- Apache Spark

## Enviroment
- Java 8

## Các bước thực hiện
### Bước 1
Clone repository

### Bước 2
Đẩy dữ liệu Stream vào Kafka

Chạy file java kafka/ProducerLogs

### Bước 3
Lấy dư liệu từ Kafka và đẩy vào HDFS

``` 
spark-submit --class kafka.ConsumerLogs --master yarn --deploy-mode client --num-executors 2 --executor-memory 1g --executor-cores 1 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 target/bigdata-project-1.0-SNAPSHOT-jar-with-dependencies.jar

```

### Bước 4
Phân tích dữ liệu

```
spark-submit --class spark.Task --master yarn --deploy-mode client --num-executors 2 --executor-memory 1g --executor-cores 1 target/bigdata-project-1.0-SNAPSHOT-jar-with-dependencies.jar

```
