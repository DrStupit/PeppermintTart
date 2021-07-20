FROM mcr.microsoft.com/dotnet/aspnet:5.0-alpine AS base
WORKDIR /app
EXPOSE 8080
 
RUN apk add icu-libs
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false
ENV ASPNETCORE_ENVIRONMENT=Development 
ENV ASPNETCORE_URLS=http://*:8080
 
FROM mcr.microsoft.com/dotnet/sdk:5.0-alpine AS build
WORKDIR /src
COPY ["ElasticLoggingService.csproj", "ElasticLoggingService/"]
RUN dotnet restore "ElasticLoggingService/ElasticLoggingService.csproj"
COPY . .
WORKDIR "/src/ElasticLoggingService"
RUN dotnet build "ElasticLoggingService.csproj" -c Release -o /app/build
 
FROM build AS publish
RUN dotnet publish "ElasticLoggingService.csproj" -c Release -o /app/publish
 
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ElasticLoggingService.dll"]