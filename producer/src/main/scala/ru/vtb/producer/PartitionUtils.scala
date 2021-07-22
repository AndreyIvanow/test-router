package ru.vtb.producer

import kafka.admin.BrokerMetadata
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.utils.SystemTime
import ru.vtb.common.Constants.DEALS_TOPIC

class PartitionUtils {

  def setPartitions(count: Int) {
    val kafkaZkClient = KafkaZkClient.apply("127.0.0.1:2181", false, 30000, 3000, 100, new SystemTime(), "kafka.server", "SessionExpireListener")

    val existingAssignments = kafkaZkClient.getPartitionAssignmentForTopics(Set(DEALS_TOPIC))
    val existingAssignment = existingAssignments.get(DEALS_TOPIC).get
    val allBrokers = kafkaZkClient.getAllBrokersInCluster
    val allBrokersMetaInfos = allBrokers.map(broker => BrokerMetadata(broker.id, broker.rack)).collect(b => b)
    val adminZkClient = new AdminZkClient(kafkaZkClient)
    adminZkClient.addPartitions(DEALS_TOPIC, existingAssignment, allBrokersMetaInfos, count)
  }

}
