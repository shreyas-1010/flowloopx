#include "flowloopx/socket_util.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstring>
#include <system_error>

namespace flx {

static void throw_errno(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

UniqueFd create_listener(const std::string& addr, uint16_t port, int backlog) {
  UniqueFd fd(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  if (!fd) throw_errno("socket");

  int one = 1;
  if (::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof one) < 0)
    throw_errno("setsockopt(SO_REUSEADDR)");

  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(port);
  if (::inet_pton(AF_INET, addr.c_str(), &sa.sin_addr) != 1) {
    throw std::system_error(EINVAL, std::generic_category(), "invalid bind address: " + addr);
  }
  if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&sa), sizeof sa) < 0) throw_errno("bind");
  if (::listen(fd.get(), backlog) < 0) throw_errno("listen");
  return fd;
}

uint16_t local_port(int fd) {
  sockaddr_in sa{};
  socklen_t len = sizeof sa;
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&sa), &len) < 0) throw_errno("getsockname");
  return ntohs(sa.sin_port);
}

void set_nodelay(int fd) {
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);  // best effort
}

}  // namespace flx
