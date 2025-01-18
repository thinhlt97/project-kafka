Project - Kafka
Mục đích:
Lấy dữ liệu từ nguồn cho trước sau đó produce vào topic trong kafka trên docker tại khóa học, sau đó consume lại dữ liệu đó, lưu thông tin vào trong mongodb
Thứ tự chạy các file:
1. consume_serve.py -> lấy thông tin từ nguồn và produce vào trong topic trên kafka
2. consumer_kafka.py -> Đọc dữ liệu từ topic vừa tạo, sau đó lưu thông tin vào trong mongodb
Kết quả chương trình:
Lấy được dữ liệu 100.000 dòng thông tin về các hành vi truy cập trên trang web glamira
