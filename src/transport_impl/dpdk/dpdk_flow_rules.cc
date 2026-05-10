/**
 * @file dpdk_flow_rules.cc
 * @brief rte_flow rules for steering UDP dst-port traffic to a specific RX queue.
 */

#ifdef ERPC_DPDK

#include <rte_flow.h>
#include "dpdk_transport.h"
#include "util/logger.h"

namespace erpc {

void DpdkTransport::install_flow_rule(size_t phy_port, size_t qp_id,
                                      uint32_t ipv4_addr, uint16_t udp_port) {
  _unused(ipv4_addr);  // Match on UDP dst port only; IP is wildcarded

  struct rte_flow_attr attr;
  memset(&attr, 0, sizeof(attr));
  attr.ingress = 1;
  attr.priority = 1;

  struct rte_flow_item_udp udp_spec, udp_mask;
  memset(&udp_spec, 0, sizeof(udp_spec));
  memset(&udp_mask, 0, sizeof(udp_mask));
  udp_spec.hdr.dst_port = rte_cpu_to_be_16(udp_port);
  udp_mask.hdr.dst_port = UINT16_MAX;

  struct rte_flow_item pattern[4];
  memset(pattern, 0, sizeof(pattern));
  pattern[0].type = RTE_FLOW_ITEM_TYPE_ETH;
  pattern[1].type = RTE_FLOW_ITEM_TYPE_IPV4;
  pattern[2].type = RTE_FLOW_ITEM_TYPE_UDP;
  pattern[2].spec = &udp_spec;
  pattern[2].mask = &udp_mask;
  pattern[3].type = RTE_FLOW_ITEM_TYPE_END;

  struct rte_flow_action_queue queue_conf;
  queue_conf.index = static_cast<uint16_t>(qp_id);

  struct rte_flow_action actions[2];
  memset(actions, 0, sizeof(actions));
  actions[0].type = RTE_FLOW_ACTION_TYPE_QUEUE;
  actions[0].conf = &queue_conf;
  actions[1].type = RTE_FLOW_ACTION_TYPE_END;

  struct rte_flow_error error;
  memset(&error, 0, sizeof(error));

  int ret = rte_flow_validate(static_cast<uint16_t>(phy_port), &attr,
                              pattern, actions, &error);
  if (ret != 0) {
    ERPC_WARN(
        "rte_flow_validate failed for port %zu queue %zu UDP dst port %u: %s\n",
        phy_port, qp_id, udp_port,
        (error.message != nullptr) ? error.message : "unknown");
    return;
  }

  struct rte_flow *flow = rte_flow_create(static_cast<uint16_t>(phy_port),
                                          &attr, pattern, actions, &error);
  if (flow == nullptr) {
    ERPC_WARN(
        "rte_flow_create failed for port %zu queue %zu UDP dst port %u: %s\n",
        phy_port, qp_id, udp_port,
        (error.message != nullptr) ? error.message : "unknown");
  } else {
    ERPC_WARN("Installed rte_flow rule: port %zu, queue %zu, UDP dst port %u\n",
              phy_port, qp_id, udp_port);
  }
}

}  // namespace erpc
#endif  // ERPC_DPDK
