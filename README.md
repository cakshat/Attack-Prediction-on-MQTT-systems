[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/yDpbj8_M)

**Video Demo and Data Links**
- Box: https://cmu.box.com/s/68efvyr2q3v8osrn0vbv13fv2ymxdqw9
- Google Drive: https://drive.google.com/drive/folders/1JWlEojlUQ2_q9UCV61Ilf3RzDz_QetiL?usp=sharing

- There are 3 folders:
-- Data, Local and Cloud.
-- Local has demo on my device where I explain in detail the code changes and functionalities.
-- Cloud has video demo where I run the tasks on cloud, point out specific changes in code.
-- Cloud folder has seperate postgressql on cloud video as well and a setup on cloud video as well.
-- In cloud folder q1_2_3_data_clean file has q1,2, and data cleaning part of q3, if also has a failed run of spark models; whose seperate video is in the file q3_spark_cloud. Data has the .csv files I have used for the project.


**Setting System up for running these notebooks on cloud and local device**
- These system requirements must be met to run on device.
1. Install python 3.11.4 https://www.python.org/downloads/ 
2. Install Java JDK (version 8 and above) [https://www.oracle.com/in/java/techno...](https://www.oracle.com/in/java/technologies/downloads/#jdk19-windows). Download in C:/Java/jdk.
3. Install Apache Spark (mine is 3.4.1) [https://spark.apache.org/downloads.html] in C:/Spark. Select the Hadoop version and download the tar file and extract it. Download the winutils file [https://github.com/steveloughran/winutils]. Put it in C:/Hadoop/bin/.
4. Set up the environment variables. Java, Hadoop, Spark and pyspark env variables must be set up properly.
5. Install Anaconda. Using Anaconda download Jupyter notebook. Code can also be run on VS Code.
6. Install PostgreSQL. https://www.postgresql.org/download/windows/
7. Install torch. I used `pip install torch --pre --extra-index-url https://download.pytorch.org/whl/nightly/cu116` as my cuda driver is 11.6 and this will allow me run models on cuda using torch.
8. The above libraries are pre-requisites for running the notebooks.
9. Use the requirements.txt file to download necessary libraries. `pip install -r requirements.txt`. If this does not work try removing the pyspark line.
10. Download the train70_reduced.csv and test30_reduced.csv from [https://www.kaggle.com/datasets/cnrieiit/mqttset] in the same directory as the notebook.
11. In all notebooks :
`import findspark`
`findspark.init()`
`findspark.find()`
must be run.
13. In db_properties["password"] update your postgressql password.
14. In Kafka config update your own bootstrap.servers, sasl.username and sasl.password and topic_name.
15. While running the PyTorch notebooks make sure to check if your system has and if it can access the GPU. This can be done by entering the following cmd prompt:
    `python`
    `import torch`
    `torch.cuda.is_available()`
    If you get True, good to go, else set-up the cuda drivers and corresponding torch version corresponding to your cuda drivers. All info for this can be found here. [https://stackoverflow.com/questions/57238344/i-have-a-gpu-and-cuda-installed-in-windows-10-but-pytorchs-torch-cuda-is-availa?noredirect=1&lq=1]

- The following changes must be donw while running code on cloud.
  1. Create the cluster, enable gateway connect and select Jupyter (and docker).
  2. In the cluster bucket add the .csv files. Then use the path provided by the cloud and update in spark.read.csv("<Path>").
  3. In all notebooks `import findspark`,`findspark.init()`,`findspark.find()` must be commented out.
  4. Set master to "yarn" or keep "local".
  5. `!pip install confluent-kafka`,`!pip install feedparser`,`!pip install nltk` should be include before running Kafka (again update your own bootstrap.servers, sasl.username and sasl.password and topic_name).
  6. For running the PyTorch notebook `!pip install torch` will be required.
 
- To run Postgres on cloud:
  1. In the cluster bucket add the .jar file. (Spark -> SparkHadoop -> jars -> postgres).
  2. Then update the spark config: `.set('spark.jars', '<PATH to the jar file (will start with gs://)>')`
  3. Create a postgresql instance on the cloud.
  4. Remember the password.
  5. In Connections -> Networks add 0.0.0.0/0.
  6. Now make these 2 changes in the postgres code: `db_properties['password']="<PASSWORD OF THE INSTANCE>"` and `db_properties['url']="jdbc:postgresql://<PUBLIC IP OF THE POSTGRESQL INSTANCE>/postgres"`

  


This is multiclass classification problem. For this problem, I have chosen Random Forest classifier and Decision Tree classifier (Spark) and shallow and deep neural network. <br>

**Decision Trees:**
- Interpretability: Decision trees are easy to understand about how they make predictions, which can be helpful for debugging and improving the model.
-	Robustness: Decision trees are relatively robust to noise and outliers in the data. This is because they do not rely on assumptions about the underlying distribution of the data.
-	Scalability: Decision trees can be efficiently trained on large datasets. This makes them a scalable algorithm that can be used for real-world problems.
-	In addition to these general advantages, decision trees also have several specific advantages for multiclass classification. For example, decision trees are able to handle classes with different sizes, and they are not susceptible to the problem of multiclass label imbalance.

**Random Forests:**
-	Accuracy: Random forests are known for their high accuracy, even on complex datasets. This is because they are able to combine the predictions of multiple decision trees, which helps to reduce overfitting and improve generalization.
-	Robustness: Random forests are also very robust to noise and outliers in the data. This is because they are able to average out the predictions of multiple trees, which helps to reduce the impact of any individual tree that is affected by noise or outliers.
-	Scalability: Random forests can be efficiently trained on large datasets. This makes them a scalable algorithm that can be used for real-world problems.
-	Feature importance: Random forests provide a natural way to measure the importance of different features in the dataset. This information can be used to improve the performance of the model by selecting the most important features.

**Neural Networks:**
-	Feature Learning: Neural networks have the ability to learn and extract relevant features from raw data, eliminating the need for manual feature engineering. This feature learning capability is particularly useful in high-dimensional datasets where identifying relevant features is challenging or time-consuming.
-	Non-Linear Relationships: Neural networks excel at capturing non-linear relationships between input features and target classes. This makes them well-suited for tasks where the decision boundary between classes is not easily represented by linear models.
-	Scalability: Neural networks can effectively handle large and complex datasets, making them suitable for real-world applications with massive amounts of data.
-	Handling Imbalanced Data: Neural networks can effectively handle imbalanced datasets, where the number of examples for different classes varies significantly. 
-	Multi-Label Classification: Neural networks can be extended to handle multi-label classification tasks, where an input can belong to multiple classes simultaneously
-	Adaptability: Neural networks can be adapted to various classification tasks by adjusting their architecture and hyperparameters. This adaptability makes them versatile tools for solving a wide range of problems.


**Decision Trees:**
| |	Untuned	 |Tuned |
|--|--| -- |
|Training Accuracy|	89.8%	 |92.84%|
|Testing Accuracy|	89.09%	|92.19%|

Parameters choosen for tuning and their effects:
- maxDepth: Can help increase the accuracy of the algorithm and identify more complex relations which may go unnoticed at shallow depths.
- maxBins: Can help increase accuracy by increasing the complexity of the tree by allowing more potential splits at each node in the tree.

**Random Forest:**
| |Untuned|Tuned|
|--|--| -- |
|Training Accuracy|86.01%|90.57%|
|Testing Accuracy|86.78%|89.8%|

Parameters choosen for tuning and their effects:
- maxDepth: Can help increase the accuracy of the algorithm and identify more complex relations which may go unnoticed at shallow depths.
- numTrees: Can help increase increase accuracy as more trees help give a more general and probabilistically accurate value (ensemble of trees) and prevent overfitting.

**Neural Network (Shallow):**

||Untuned|Tuned|
|--|--|--|
|Validation Accuracy|82.12%|82.12%|
|Testing Accuracy|82.01%|82.02%|

Parameters choosen for tuning and their effects:
- learning rate: Help prevent over & under fitting
- epochs: Help optimize accuracy and time taken to train the model.
- width of hidden layer: Increasing complexity may allow for more accurate results.

**Neural Network (Deep):**

||Untuned|Tuned|
|--|--|--|
|validation Accuracy|82.11%|82.13%|
|Testing Accuracy|82.01%|82.02%|


**Comparing the models**
1. Performace (Accuracy): Decision Tree > Random Forest > Shallow Neural Network ~ Deep Neural Network.
2. Both decision trees and random forests showed small but noticable improvements after tuning. DT went from 89% range to 92% range and RF went from 86% to 89%-90% range.
3. In contrast Neural Networks did not show improvement after tuning.
4. Best model was the decision tree which performed better than random forest. This is probably because RF usually surpass DT by preventing overfitting but here fit itself was difficult (train acc did not go close to 98% or 99%) which leans towards overfit.
5. Neural networks did a terrible job of fitting. (Not shown here) I tried neural network for 4-5 layers and much mor epeochs still the accuracy was stagnated at 82% mark.



I am using reduced version of the data. In the code and notebook, the latest one that I have uploaded does not have any detected target words. In a prev version (img attached) only dos/ddos is detected.
Remarks are from observing the dataset and Constraints have been written based on observation and information from internet along with descriptions. For some variables they do not match.

| Name | Constraints | Remarks from Observation | Description |
| --- | --- | --- | --- |
| tcp_flags | There are 6 types of flags | 8 distinct Hex values | TCP flags are a part of the TCP header, and they provide control information about the TCP packet |
| tcp_time_delta | Double | There is one -ve value which is odd. | It measures the time it takes for a particular event or packet to occur after another related event or packet in a TCP (Transmission Control Protocol) communication session. |
| tcp_len | The tcp_len field is a 2-byte integer value that can take any value from 20 to 65535 bytes. | Whole numbers (~0 to 32000) | It represents the total number of bytes in a specific TCP packet, including both the TCP header and the payload data. |
| mqtt_conack_flags | mqtt_conack_flags field is a 1-byte integer value that can take (0 to 5) | 0 and 0x00 | The mqtt_conack_flags field is part of the MQTT CONNECT and CONNACK messages, which are exchanged between a client and a broker during the initial connection setup. These flags are used to negotiate and establish parameters of the MQTT connection. |
| mqtt_conack_flags_reserved | Whole number | (only 0) | MQTT messages may include reserved bits or fields that are not currently defined or utilized but are included to ensure compatibility with future versions or extensions of the protocol. The mqtt_conack_flags_reserved is an example of such a reserved field. Its purpose is to allow for potential future uses or extensions without requiring changes to the core MQTT protocol. |
| mqtt_conack_flags_sp | Can only have 0 or 1 values |  | Stands for "Session Present." It is a single bit flag that indicates whether the MQTT broker has stored session state data for the client. 1 → Session present, 0 → session not present                                                         |
| mqtt_conack_val | Whole number  | (0 and 5) | Connection Acknowledge Value |
| mqtt_conflag_cleansess | Boolean value, and it must be set to true or false. | (0 and 1) | Controls whether or not the broker should clean the client's session when it disconnects. |
| mqtt_conflag_passwd | Boolean value, and it must be set to true or false. | (0 and 1) | Whether or not the broker requires the client to provide a password to authenticate. If this flag is set to true, the client must provide a valid username and password when connecting to the broker. |
| mqtt_conflag_qos | 3 values: (0,1,2) | (0) | MQTT CONNFLAG QoS (Quality of Service) is a flag that controls the quality of service level for the connection between the client and the broker. Used to control the reliability of message delivery between the client and the broker. The appropriate value to set for this flag will depend on the specific needs of the application. 0 → At most once delivery.This is the lowest quality of service level, and it means that the broker will make a best effort to deliver messages to the client, but it is not guaranteed. Messages may be lost or duplicated. 1 → At least once delivery. This is the middle quality of service level, and it means that the broker will guarantee that each message is delivered to the client at least once. However, messages may be duplicated. 2 → Exactly once delivery. This is the highest quality of service level, and it means that the broker will guarantee that each message is delivered to the client exactly onc |
| mqtt_conflag_reserved | Boolean value, and it must be set to false. | (0) | mqtt_conflag_reserved is a flag in the MQTT protocol that is reserved for future use. This means that the flag should not be set by clients or brokers. If the flag is set, the broker may disconnect the client or reject the connection. |
| mqtt_conflag_retain | Boolean value, and it must be set to true or false. | (0) | Controls whether or not a message is retained by the broker. If this flag is set to true, the broker will retain the message and deliver it to any new subscribers that subscribe to the topic. If this flag is set to false, the broker will not retain the message, and new subscribers will not receive the message. |
| mqtt_conflag_uname | Boolean value, and it must be set to true or false. | (0 and 1) | MQTT CONNFLAG Username is a flag in the MQTT protocol that controls whether or not the client provides a username when connecting to the broker. |
| mqtt_conflag_willflag | Boolean value, and it must be set to true or false. | (0) | Controls whether or not the client publishes a will message when it disconnects from the broker. will message can be used to indicate the reason for the client's disconnection. This can be helpful for debugging and troubleshooting purposes. |
| mqtt_conflags | Boolean value, and it must be set to true or false. | Hex values | mqtt_conflags is a field in the MQTT CONNECT packet that contains a set of flags that control the behavior of the connection |
| mqtt_dupflag | Boolean value, and it must be set to true or false. | (0 and 1) | mqtt_dupflag is a flag in the MQTT PUBLISH packet that controls whether or not the message is a duplicate. |
| mqtt_hdrflags | Boolean | Hex values | mqtt_hdrflags, also known as MQTT header flags, is a field in the MQTT packet header that contains a set of flags that control the behavior of the packet. The values that each mqtt_hdrflags flag can take are: DUP: true or false;  QOS: 0, 1, or 2; RETAIN: true or false |
| mqtt_kalive | Must be a positive integer value, and it must be specified in seconds. The default value for mqtt_kalive is 60 seconds. | Whole nums | Controls the maximum amount of time that a client can be idle before the broker disconnects the client. The full form of mqtt_kalive is "MQTT Keep Alive". |
| mqtt_len | non-negative integer value, and it must be less than the maximum packet size supported by the broker. The maximum packet size supported by the broker is typically 268,435,455 bytes. |  | Specifies the length of the MQTT packet payload. The MQTT packet payload is the section of the MQTT packet that contains the data that is being transmitted between the client and the broker. The payload can contain any type of data, such as strings, JSON, XML, or binary data. |
| mqtt_msg | MQTT messages can include alphanumeric characters, symbols, and even binary data. | Contain normal and very big numbers order of 10^203 and some strings as well | mqtt_msg is a structure in the MQTT protocol that represents a message. The structure contains the following fields: topic: The topic that the message is being published to. qos: The quality of service level for the message. retain: A flag that indicates whether or not the message should be retained by the broker. payload: The data that is being transmitted in the message. |
| mqtt_msgid | The mqtt_msgid field is a 16-bit integer value that is generated by the client. The client must ensure that the mqtt_msgid value is unique for each message that it publishes. | Whole numbers but they repeat… | MQTT Message Identifier, is a field in the MQTT PUBLISH packet that is used to uniquely identify the message. |
| mqtt_msgtype | Should have values bet 0x01 to 0x0E i.e. 1 to 14 | Here 0 also present | MQTT Message Type, is a field in the MQTT packet header that specifies the type of MQTT packet. |
| mqtt_proto_len | The mqtt_proto_len field is a 2-byte integer value that can take any value from 0 to 65535. | (0 and 4) | Specifies the length of the MQTT packet, including the header and the payload |
| mqtt_protoname | Nums and string | (0 and ‘MQTT’) | MQTT Prototype name  |
| mqtt_qos | The mqtt_qos field is a 1-bit integer value that can take one of the following values: QoS 0: 0; QoS 1: 1; QoS 2: 2 | (0 and 1) | Field in the MQTT packet header that specifies the level of quality of service for the message. The full form of mqtt_qos is "MQTT Quality of Service” |
| mqtt_retain | Boolean value, and it must be set to true or false. | (0 and 1) | Controls whether or not the broker should retain the client's subscription. mqtt_retain flag applies to individual messages. |
| mqtt_sub_qos | 0, 1, 2 | (0) | Controls the quality of service level for the subscription. |
| mqtt_suback_qos | The mqtt_suback_qos field is a 1-bit integer value that can take one of the following values: QoS 0: 0; QoS 1: 1; QoS 2: 2 | (0) | MQTT Subscription Acknowledgement Quality of Service, is a field in the MQTT SUBACK packet that specifies the quality of service level for the subscription. |
| mqtt_ver | For 3.1 → 4, For 5 → 5 | (0 and 4) | Specifies the version of the MQTT protocol that the client is using. |
| mqtt_willmsg | Whole nums | (0) | MQTT Will Message, is a message that is published by the client to the broker when the client disconnects unexpectedly. The mqtt_willmsg should be used whenever it is important to notify other clients of the unexpected disconnection of a client. |
| mqtt_willmsg_len | 2-byte integer value that can take any value from 0 to 65535. | (0) | Specifies the length of the MQTT Will Message payload. |
| mqtt_willtopic | The mqtt_willtopic field is a string that can take any value that is a valid MQTT topic. | (0) | MQTT Will Topic, is the topic that the MQTT Will Message is published to when a client disconnects unexpectedly. |
| mqtt_willtopic_len | Whole nums | (0) | MQTT Will Topic length |
| target | Strings |  | Type of attack |





