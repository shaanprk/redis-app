#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <cctype>
#include <algorithm>
#include <unistd.h>
#include <vector>
#include <unordered_map>
#include <mutex>

std::unordered_map<std::string, std::string> key_value_store;
std::unordered_map<std::string, std::chrono::steady_clock::time_point> expiration_times;
std::mutex store_mutex;

void handle_client(int client_fd) {
    char buffer[1024] = {0}; // Input buffer
    while (true) {
        int bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0); // Leave space for null terminator
        if (bytes_received <= 0) {
            // Connection closed or error occurred
            break;
        }

        buffer[bytes_received] = '\0';
        std::string input(buffer);
        std::cout << "Received raw input: " << input;

        // Parse Redis protocol input
        size_t pos = 0;
        std::string command;
        std::vector<std::string> arguments;

        // Parse number of arguments (e.g., "*3")
        if (input[pos] == '*') {
            pos = input.find("\r\n", pos);
            if (pos == std::string::npos) break; // Malformed input
            pos += 2; // Skip "\r\n"
        }

        // Parse command and arguments
        while (pos < input.size() && input[pos] == '$') {
            // Skip the "$<len>\r\n" part
            pos = input.find("\r\n", pos);
            if (pos == std::string::npos) break; // Malformed input
            pos += 2; // Skip "\r\n"

            // Extract the next token
            size_t next_pos = input.find("\r\n", pos);
            if (next_pos == std::string::npos) break; // Malformed input
            arguments.push_back(input.substr(pos, next_pos - pos));
            pos = next_pos + 2; // Skip "\r\n"
        }

        // Ensure at least one argument (the command) is provided
        if (arguments.empty()) {
            std::string response = "-ERR malformed command\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
            continue;
        }

        // Extract the command and convert it to uppercase
        command = arguments[0];
        std::transform(command.begin(), command.end(), command.begin(), [](unsigned char c) { return std::toupper(c); });

        // Handle recognized commands
        if (command == "SET") {
            if (arguments.size() < 3 || arguments.size() > 5) {
                std::string response = "-ERR wrong number of arguments for 'SET'\r\n";
                send(client_fd, response.c_str(), response.size(), 0);
                continue;
            }

            std::string key = arguments[1];
            std::string value = arguments[2];
            long long expiry_ms = -1;

            // Parse optional PX argument
            if (arguments.size() == 5) {
                std::string option = arguments[3];
                std::transform(option.begin(), option.end(), option.begin(), [](unsigned char c) { return std::toupper(c); });
                if (option == "PX") {
                    try {
                        expiry_ms = std::stoll(arguments[4]);
                    } catch (...) {
                        std::string response = "-ERR invalid PX argument\r\n";
                        send(client_fd, response.c_str(), response.size(), 0);
                        continue;
                    }
                } else {
                    std::string response = "-ERR unknown option\r\n";
                    send(client_fd, response.c_str(), response.size(), 0);
                    continue;
                }
            }

            {
                std::lock_guard<std::mutex> lock(store_mutex);
                key_value_store[key] = value;
                if (expiry_ms > 0) {
                    expiration_times[key] = std::chrono::steady_clock::now() + std::chrono::milliseconds(expiry_ms);
                } else {
                    expiration_times.erase(key);
                }
            }

            std::string response = "+OK\r\n";
            send(client_fd, response.c_str(), response.size(), 0);

        } else if (command == "GET") {
            if (arguments.size() != 2) {
                std::string response = "-ERR wrong number of arguments for 'GET'\r\n";
                send(client_fd, response.c_str(), response.size(), 0);
                continue;
            }

            std::string key = arguments[1];
            std::string value;
            bool is_expired = false;

            {
                std::lock_guard<std::mutex> lock(store_mutex);
                auto it = expiration_times.find(key);
                if (it != expiration_times.end() && std::chrono::steady_clock::now() > it->second) {
                    // Key has expired
                    key_value_store.erase(key);
                    expiration_times.erase(it);
                    is_expired = true;
                } else {
                    auto kv_it = key_value_store.find(key);
                    if (kv_it != key_value_store.end()) {
                        value = kv_it->second;
                    }
                }
            }

            if (is_expired || value.empty()) {
                std::string response = "$-1\r\n"; // Null bulk reply
                send(client_fd, response.c_str(), response.size(), 0);
            } else {
                std::string response = "+" + value + "\r\n";
                send(client_fd, response.c_str(), response.size(), 0);
            }

        } else {
            std::string response = "-ERR unknown command\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
        }
    }

    close(client_fd);
}

int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";

  // You can use print statements as follows for debugging, they'll be visible when running tests.
  // std::cout << "Logs from your program will appear here!\n";

  while (true) {
    int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
    std::cout << "Client connected\n";
    std::thread new_client(handle_client, client_fd);
    new_client.detach();
  }
  
  close(server_fd);

  return 0;
}
