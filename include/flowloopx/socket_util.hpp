#pragma once
#include <cstdint>
#include <string>

#include "flowloopx/fd.hpp"

namespace flx {

// All helpers throw std::system_error on failure.
UniqueFd create_listener(const std::string& addr, uint16_t port, int backlog);
uint16_t local_port(int fd);
void set_nodelay(int fd);

}  // namespace flx
