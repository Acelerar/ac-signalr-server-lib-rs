using Microsoft.AspNetCore.SignalR.Client;
using System.Text.Json;

Console.WriteLine("SignalR Chat Client");
Console.WriteLine("===================\n");

// Create the connection to the SignalR hub
var connection = new HubConnectionBuilder()
    .WithUrl("http://127.0.0.1:3000/chat")
    .WithAutomaticReconnect()
    .Build();

// Keep track of the streaming task
CancellationTokenSource? streamCts = null;

// Register handler for receiving messages from the server (for backwards compatibility)
connection.On<string, string>("ReceiveMessage", (user, message) =>
{
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {user}: {message}");
});

// Handle connection closed
connection.Closed += async (error) =>
{
    if (error != null)
    {
        Console.WriteLine($"Connection closed with error: {error.Message}");
    }
    streamCts?.Cancel();
    await Task.Delay(5000);
    try
    {
        await connection.StartAsync();
        streamCts = new CancellationTokenSource();
        _ = StartMessageStreamAsync(connection, streamCts.Token);
    }
    catch
    {
        // Ignore reconnection errors
    }
};

try
{
    // Start the connection
    Console.WriteLine("Connecting to server...");
    await connection.StartAsync();
    Console.WriteLine("Connected!");
    Console.WriteLine("Type messages and press Enter to send (or 'exit' to quit)");
    Console.WriteLine();

    // Get and display the connection ID
    var connectionId = await connection.InvokeAsync<string>("GetConnectionId");
    Console.WriteLine($"Your connection ID: {connectionId}\n");

    // Start streaming messages from the server
    streamCts = new CancellationTokenSource();
    _ = StartMessageStreamAsync(connection, streamCts.Token);

    // Main message loop
    while (true)
    {
        var input = Console.ReadLine();
        
        if (string.IsNullOrWhiteSpace(input))
            continue;

        if (input.Equals("exit", StringComparison.OrdinalIgnoreCase))
            break;

        // Send the message to the server
        try
        {
            var result = await connection.InvokeAsync<object>("SendMessage", "DotNetClient", input);
            // Messages will be received via the ReceiveMessages stream
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error sending message: {ex.Message}");
        }
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
    if (ex.InnerException != null)
    {
        Console.WriteLine($"Inner Exception: {ex.InnerException.Message}");
    }
}
finally
{
    // Clean up
    streamCts?.Cancel();
    await connection.StopAsync();
    await connection.DisposeAsync();
    Console.WriteLine("Disconnected.");
}

async Task StartMessageStreamAsync(HubConnection connection, CancellationToken cancellationToken)
{
    try
    {
        Console.WriteLine("Starting message stream...");
        var stream = connection.StreamAsync<JsonElement>("ReceiveMessages", cancellationToken);
        
        await foreach (var message in stream.WithCancellation(cancellationToken))
        {
            if (message.TryGetProperty("user", out var userProp) && 
                message.TryGetProperty("message", out var msgProp))
            {
                var user = userProp.GetString() ?? "Unknown";
                var msg = msgProp.GetString() ?? "";
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {user}: {msg}");
            }
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("Message stream cancelled.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Stream error: {ex.Message}");
    }
}
