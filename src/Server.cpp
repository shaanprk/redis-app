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
#include <vector>
#include <unordered_map>
#include <mutex>
#include <algorithm>
#include <chrono>

std::unordered_map<std::string, std::string> key_value_store;
std::unordered_map<std::string, std::chrono::steady_clock::time_point> expiration_times;
std::mutex store_mutex;

// Function to parse the Redis protocol input
std::vector<std::string> parse_input(const std::string &input) {
    size_t pos = 0;
    std::vector<std::string> arguments;

    // Parse number of arguments (e.g., "*3")
    if (input[pos] == '*') {
        pos = input.find("\r\n", pos);
        if (pos == std::string::npos) return arguments; // Malformed input
        pos += 2; // Skip "\r\n"
    }

    // Parse command and arguments
    while (pos < input.size() && input[pos] == '$') {
        pos = input.find("\r\n", pos);
        if (pos == std::string::npos) break; // Malformed input
        pos += 2; // Skip "\r\n"

        size_t next_pos = input.find("\r\n", pos);
        if (next_pos == std::string::npos) break; // Malformed input
        arguments.push_back(input.substr(pos, next_pos - pos));
        pos = next_pos + 2; // Skip "\r\n"
    }

    return arguments;
}

// Function to clean expired keys
void check_and_clean_expired(const std::string &key) {
    auto it = expiration_times.find(key);
    if (it != expiration_times.end() && std::chrono::steady_clock::now() > it->second) {
        key_value_store.erase(key);
        expiration_times.erase(it);
    }
}

// Function to handle SET command
std::string handle_set(const std::vector<std::string> &arguments) {
    if (arguments.size() < 3 || arguments.size() > 5) {
        return "-ERR wrong number of arguments for 'SET'\r\n";
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
                return "-ERR invalid PX argument\r\n";
            }
        } else {
            return "-ERR unknown option\r\n";
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

    return "+OK\r\n";
}

// Function to handle GET command
std::string handle_get(const std::vector<std::string> &arguments) {
    if (arguments.size() != 2) {
        return "-ERR wrong number of arguments for 'GET'\r\n";
    }

    std::string key = arguments[1];
    std::string value;

    {
        std::lock_guard<std::mutex> lock(store_mutex);
        check_and_clean_expired(key);

        auto kv_it = key_value_store.find(key);
        if (kv_it != key_value_store.end()) {
            value = kv_it->second;
        }
    }

    if (value.empty()) {
        return "$-1\r\n"; // Null bulk reply
    }

    return "+" + value + "\r\n";
}

// Function to handle unknown commands
std::string unknown_command() {
    return "-ERR unknown command\r\n";
}

// Function to handle individual client connections
void handle_client(int client_fd) {
    char buffer[1024] = {0};

    while (true) {
        int bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) {
            break; // Connection closed or error occurred
        }

        buffer[bytes_received] = '\0';
        std::string input(buffer);
        std::vector<std::string> arguments = parse_input(input);

        if (arguments.empty()) {
            std::string response = "-ERR malformed command\r\n";
            send(client_fd, response.c_str(), response.size(), 0);
            continue;
        }

        std::string command = arguments[0];
        std::transform(command.begin(), command.end(), command.begin(), [](unsigned char c) { return std::toupper(c); });

        std::string response;
        if (command == "SET") {
            response = handle_set(arguments);
        } else if (command == "GET") {
            response = handle_get(arguments);
        } else {
            response = unknown_command();
        }

        send(client_fd, response.c_str(), response.size(), 0);
    }

    close(client_fd);
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    if (listen(server_fd, 5) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    std::cout << "Server is running on port 6379\n";

    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
        if (client_fd >= 0) {
            std::thread(handle_client, client_fd).detach();
        }
    }

    close(server_fd);
    return 0;
}
