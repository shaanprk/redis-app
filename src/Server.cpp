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
std::string server_role = "master";
bool is_master = true;
std::string replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
int offset = 0;
std::string master_host;
int master_port;

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

// Function to handle DEL command
std::string handle_del(const std::vector<std::string> &arguments) {
    if (arguments.size() < 2) {
        return "-ERR wrong number of arguments for 'DEL'\r\n";
    }

    int keys_deleted = 0;
    {
        std::lock_guard<std::mutex> lock(store_mutex);
        for (size_t i = 1; i < arguments.size(); ++i) {
            std::string key = arguments[i];
            if (key_value_store.erase(key) > 0) {
                expiration_times.erase(key);
                ++keys_deleted;
            }
        }
    }

    return ":" + std::to_string(keys_deleted) + "\r\n";
}

// Function to handle PING command
std::string handle_ping(const std::vector<std::string> &arguments) {
    if (arguments.size() == 1) {
        return "+PONG\r\n";
    } else if (arguments.size() == 2) {
        return "+" + arguments[1] + "\r\n";
    } else {
        return "-ERR wrong number of arguments for 'PING'\r\n";
    }
}

// Function to handle ECHO command
std::string handle_echo(const std::vector<std::string> &arguments) {
    if (arguments.size() != 2) {
        return "-ERR wrong number of arguments for 'ECHO'\r\n";
    }
    return "$" + std::to_string(arguments[1].size()) + "\r\n" + arguments[1] + "\r\n";
}

// Function to handle INFO command
std::string handle_info(const std::vector<std::string> &arguments) {
    std::string response = is_master ? "role:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0" : "role:slave";
    return "+" + response + "\r\n"; 
}

// Function to handle REPCLONF command
std::string handle_repclonf(const std::vector<std::string> &arguments) {
    if (arguments.size() < 3) {
        return "-ERR wrong number of argumetns for 'REPCLONF'\r\n";
    }

    std::string subcommand = arguments[1];
    std::string response = "+OK\r\n";
    
    {
        std::lock_guard<std::mutex> lock(store_mutex);

        // Subcommand: listening-port
        if (subcommand == "listening-port") {
            try {
                int port = std::stoi(arguments[2]);
                listening_port = port; // Store or handle the listening port
                // Optionally log or apply configuration changes here
            } catch (...) {
                return "-ERR invalid port number\r\n";
            }
        }
        // Subcommand: capa (capabilities)
        else if (subcommand == "capa") {
            std::string capability = arguments[2];
            if (capability == "psync2") {
                support_psync2 = true; // Enable PSYNC v2 if applicable
            } else {
                return "-ERR unsupported capability\r\n";
            }
        }
        // Unknown subcommand
        else {
            return "-ERR unknown REPLCONF subcommand\r\n";
        }
    }

    return response;

}

// Function to handle unknown commands
std::string unknown_command() {
    return "-ERR unknown command\r\n";
}

void send_ping_to_master() {
    // Create socket to connect to the master server
    int master_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_fd < 0) {
        std::cerr << "Failed to create socket for replica to master connection\n";
        return;
    }

    struct sockaddr_in master_addr;
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(master_port);  // Use master port

    // Resolve master IP address
    struct hostent *host = gethostbyname(master_host.c_str());
    if (host == nullptr) {
        std::cerr << "Failed to resolve master host\n";
        close(master_fd);
        return;
    }
    memcpy(&master_addr.sin_addr.s_addr, host->h_addr_list[0], host->h_length);

    // Connect to the master server
    if (connect(master_fd, (struct sockaddr *)&master_addr, sizeof(master_addr)) < 0) {
        std::cerr << "Failed to connect to master\n";
        close(master_fd);
        return;
    }

    // Send PING command to master
    std::string ping_message = "*1\r\n$4\r\nPING\r\n";
    if (send(master_fd, ping_message.c_str(), ping_message.size(), 0) < 0) {
        std::cerr << "Failed to send PING command\n";
        close(master_fd);
        return;
    }

    // Receive response from master
    char buffer[1024] = {0};
    int bytes_received = recv(master_fd, buffer, sizeof(buffer) - 1, 0);
    if (bytes_received > 0) {
        buffer[bytes_received] = '\0';
        std::string response(buffer);
        if (response == "+PONG\r\n") {
            std::cout << "Received PONG from master\n";
        } else {
            std::cerr << "Unexpected response from master: " << response << "\n";
        }
    } else {
        std::cerr << "Failed to receive response from master\n";
    }

    // Close the socket after communication
    close(master_fd);
}

// Function to handle individual client connections
void handle_client(int client_fd) {
    char buffer[1024] = {0};

    while (true) {
        int bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) {
            close(client_fd);
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
        } else if (command == "DEL") {
            response = handle_del(arguments);
        } else if (command == "PING") {
            response = handle_ping(arguments);
        } else if (command == "ECHO") {
            response = handle_echo(arguments);
        } else if (command == "INFO") {
            response = handle_info(arguments);
        } else if (command == "REPCLONF") {
            response = handle_repclonf(arguments);    
        }else {
            response = unknown_command();
        }

        send(client_fd, response.c_str(), response.size(), 0);
    }

    close(client_fd);
}

int main(int argc, char **argv) {
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

    int port = 6379;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0) {
            port = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--replicaof") == 0) {
            is_master = false;
            std::string host_and_port = argv[++i];
            master_host = host_and_port.substr(0, host_and_port.find(" "));
            master_port = std::stoi(host_and_port.substr(host_and_port.find(" ") + 1, host_and_port.size()));
        }
    }

    server_addr.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    if (listen(server_fd, 5) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    if (!is_master) {
        send_ping_to_master();
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
