# .NET SignalR Chat Client

This is a simple .NET console application that demonstrates how to connect to the tt-signalr-server chat example using the official Microsoft.AspNetCore.SignalR.Client library.

## Prerequisites

- [.NET 9.0 SDK](https://dotnet.microsoft.com/download) or later

## Running the Client

1. First, start the Rust SignalR server from the repository root:
   ```bash
   cargo run --example chat_server
   ```

2. In a separate terminal, navigate to the client directory and run:
   ```bash
   cd clients/dotnet-chat-client/ChatClient
   dotnet run
   ```

3. Once connected, you can:
   - Type messages and press Enter to send them
   - Receive messages from the server
   - Type `exit` to quit

## Features

- Connects to the SignalR hub at `http://127.0.0.1:3000/chat`
- Receives messages through the `ReceiveMessage` callback
- Sends messages using the `SendMessage` method
- Retrieves the connection ID using the `GetConnectionId` method
- Automatic reconnection on connection loss

## How It Works

The client:
1. Establishes a connection to the SignalR hub
2. Registers a handler for `ReceiveMessage` to display messages from the server
3. Sends user input as messages via the `SendMessage` method with username "DotNetClient"
4. Handles disconnections gracefully with automatic reconnection

## Example Session

```
SignalR Chat Client
===================

Connecting to server...
Connected!
Type messages and press Enter to send (or 'exit' to quit)

Your connection ID: 12345678-1234-1234-1234-123456789012

[10:30:15] Server: Welcome! Your connection ID is 12345678-1234-1234-1234-123456789012
Hello from .NET!
[10:30:20] DotNetClient: Hello from .NET!
Testing the chat server
[10:30:25] DotNetClient: Testing the chat server
exit
Disconnected.
```
