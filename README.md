# 这个MQTT_sub实现提供了以下特性：
1. 根据配置文件的ip、端口、用户名、密码，连接到mqtt broker，并处理好断线重连。订阅config.json中第一层字典的所有key；
2. 订阅获取的消息是json格式，当收到订阅消息，在内存中存储每个订阅topic获取的json消息的实时值，对某个topic来说，对实时值作如下解释：
2.1 同一topic，消息json格式相同；
2.2 在config.json中获取topic对应的key，在订阅消息中找到key的层级，然后若新消息过来，若这个层级有重复的key，则更新此key的value，其它部分合并，否则，合并新消息和原来内存的json为新的json；
3. 对某个topic，在config.json中找到对应的api，使用此api作为http get请求的路由，用于获取此topic对应json消息的实时值；
4. 当收到订阅消息，将topic的key所在的层级及包含的更深层级的数据存储到mysql数据库，库表名从config.json获取。这一层级及包含的更深层级的字典的key作为mysql表的列，字典的value作为值，且表含有自增id作为主键，topic的key作为唯一索引；
5. 程序启动时读取config.json中所有的表的数据。当第一次收到某个topic的订阅消息时，需结合读取的表数据和收到的json数据来得到实时的json数据。表数据和json层级的对应在4）已说过，要注意，把表数据合并进json消息时，若某个field在json消息中不存在，则直接合并，若存在，则以json消息中的为准，不修改，因为订阅获取的消息才是最新的数据。
