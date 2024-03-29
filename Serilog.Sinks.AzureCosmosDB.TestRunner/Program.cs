﻿// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using System.Dynamic;
using Microsoft.Extensions.Configuration;
using Serilog;
using Serilog.Sinks.AzureCosmosDB;

IConfiguration configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json")
    .Build();


Serilog.Debugging.SelfLog.Enable(msg =>
{
    Debug.WriteLine(msg);
});
Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .Destructure.ByTransforming<ExpandoObject>(o => new Dictionary<string, object>(o))
    .WriteTo.AzureCosmosDB(new AzureCosmosDbSinkOptions()
    {
        EndpointUri = new System.Uri(configuration["AppSettings:AzureCosmosUri"]),
        AuthorizationKey = configuration["AppSettings:AzureCosmosKey"]
    })
    .CreateLogger();

while (true)
{
    Log.Information(
        "{@log} - Email: {email}, User ID: {userId}, Organization: {organization}, Organization ID: {orgId}, First Name: {firstName}, " +
        "Last Name: {lastName}, Platform: {platform}, Environment: {environment}, IP: {ip}, User-Agent: {agent}, " +
        "Screen Width: {screenWidth}, Screen Height: {screenHeight}, Window Width: {windowWidth}, Window Height: {windowHeight}",
        new ExpandoObject()
        {
            
        }, "test@test.com", 1, "Demo V2", 1, "test", "user",
        "Web", "Local", "192.168.1.1", "something", 1920, 1080, 1920, 1080);
    await Task.Delay(1000);
}