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
int listening_port;
const std::string empty_rdb = "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";

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

// Function to handle REPLCONF command
std::string handle_replconf(const std::vector<std::string> &arguments) {
    std::string response = "+OK\r\n";
    return response;
}

// Function to handle PSYNC command
std::string handle_psync(const std::vector<std::string> &arguments) {
    std::string response = "+FULLRESYNC " + replication_id + " " + std::to_string(offset) + "\r\n";
    response += "$" + std::to_string(empty_rdb.length()) + "\r\n" + empty_rdb;
    return response;
}

// Function to handle unknown commands
std::string unknown_command() {
    return "-ERR unknown command\r\n";
}

void replica_handshake() {
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
        std::cerr << "Failed to send PING to master\n";
        close(master_fd);
        return;
    }

    // Receive response from master
    char buffer[1024];
    int bytes_received = recv(master_fd, buffer, sizeof(buffer), 0);
    if (bytes_received < 0) {
        std::cerr << "Failed to receive response from master\n";
        close(master_fd);
        return;
    }

    // Print the response (just for debugging)
    std::cout << "Received from master: " << std::string(buffer, bytes_received) << "\n";

    // Send REPLCONF and PSYNC commands to master
    std::string replconf_message = "*2\r\n$7\r\nREPLCONF\r\n$5\r\nslave\r\n";
    send(master_fd, replconf_message.c_str(), replconf_message.size(), 0);

    std::string psync_message = "*2\r\n$5\r\nPSYNC\r\n$0\r\n\r\n";
    send(master_fd, psync_message.c_str(), psync_message.size(), 0);

    // Handle syncing data (not implemented in this simplified example)
    close(master_fd);
}

void handle_client(int client_socket) {
    // Buffer to store the client request
    char buffer[1024];

    // Read client request
    int bytes_received = recv(client_socket, buffer, sizeof(buffer), 0);
    if (bytes_received <= 0) {
        close(client_socket);
        return;
    }

    std::string input(buffer, bytes_received);
    std::vector<std::string> arguments = parse_input(input);

    std::string response;

    // Match the command and call the corresponding handler
    if (arguments[0] == "SET") {
        response = handle_set(arguments);
    } else if (arguments[0] == "GET") {
        response = handle_get(arguments);
    } else if (arguments[0] == "DEL") {
        response = handle_del(arguments);
    } else if (arguments[0] == "PING") {
        response = handle_ping(arguments);
    } else if (arguments[0] == "ECHO") {
        response = handle_echo(arguments);
    } else if (arguments[0] == "INFO") {
        response = handle_info(arguments);
    } else if (arguments[0] == "REPLCONF") {
        response = handle_replconf(arguments);
    } else if (arguments[0] == "PSYNC") {
        response = handle_psync(arguments);
    } else {
        response = unknown_command();
    }

    // Send the response to the client
    send(client_socket, response.c_str(), response.size(), 0);
    close(client_socket);
}

int main(int argc, char **argv) {
    // Check if there is a role (master/slave) specified
    if (argc < 3) {
        std::cerr << "Usage: ./redis_server <master/slave> <port>\n";
        return 1;
    }

    server_role = argv[1];
    listening_port = std::stoi(argv[2]);

    if (server_role == "slave") {
        if (argc != 5) {
            std::cerr << "Usage for slave: ./redis_server slave <port> <master_host> <master_port>\n";
            return 1;
        }
        master_host = argv[3];
        master_port = std::stoi(argv[4]);

        // Start the replication handshake with the master
        std::thread(replica_handshake).detach();
    }

    // Create socket to listen for connections
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create socket\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(listening_port);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Failed to bind socket\n";
        return 1;
    }

    if (listen(server_fd, 5) < 0) {
        std::cerr << "Failed to listen on socket\n";
        return 1;
    }

    std::cout << "Server listening on port " << listening_port << "...\n";

    // Main server loop to accept and handle clients
    while (true) {
        int client_socket = accept(server_fd, nullptr, nullptr);
        if (client_socket < 0) {
            std::cerr << "Failed to accept client connection\n";
            continue;
        }

        std::thread(handle_client, client_socket).detach();
    }

    return 0;
}
