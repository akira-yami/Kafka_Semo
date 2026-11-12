param(
    [ValidateSet('All', 'Normal', 'SameKey', 'Retry', 'Batch')]
    [string]$Scenario = 'All',
    [string]$BaseUrl = 'http://localhost:8082'
)

$ErrorActionPreference = 'Stop'
$api = $BaseUrl.TrimEnd('/') + '/api/orders'

function Invoke-DemoOrder {
    param([string]$Path, [int]$Count)

    $response = Invoke-RestMethod -Uri ($api + $Path) -Method Post -TimeoutSec 30
    if (@($response.records).Count -ne $Count -or $response.requestedCount -ne $Count) {
        throw "Expected $Count broker confirmations from $api$Path."
    }
    return $response
}

try {
    if ($Scenario -in 'All', 'Normal') {
        Write-Host '1. Normal order (look for the same orderId in all three consumer logs)'
        $result = Invoke-DemoOrder -Path '?amount=500000' -Count 1
        $result.records | Format-Table orderId, eventStatus, partition, offset -AutoSize
    }

    if ($Scenario -in 'All', 'SameKey') {
        Write-Host '2. Same key (one partition and increasing offsets)'
        $key = 'DEMO-' + [Guid]::NewGuid().ToString('N')
        $result = Invoke-DemoOrder -Path "/same-key?key=$key" -Count 3
        $records = $result.records
        $statuses = ($records | ForEach-Object { $_.eventStatus }) -join ','
        $partitions = @($records | Group-Object partition)
        if ($partitions.Count -ne 1 -or $statuses -ne 'CREATED,PAID,DELIVERED' -or
            $records[0].offset -ge $records[1].offset -or $records[1].offset -ge $records[2].offset) {
            throw "Unexpected order or partition for key $key."
        }
        $records | Format-Table orderId, eventStatus, partition, offset -AutoSize
    }

    if ($Scenario -in 'All', 'Retry') {
        Write-Host '3. Retry and DLT (Billing attempts 3 times, with two 1-second delays)'
        $result = Invoke-DemoOrder -Path '/error' -Count 1
        $result.records | Format-Table orderId, eventStatus, partition, offset -AutoSize
        Write-Host "Find key $($result.orderId) in order-events-dlt or in the [DLT] log."
    }

    if ($Scenario -in 'All', 'Batch') {
        Write-Host '4. Batch (actual partition distribution)'
        $result = Invoke-DemoOrder -Path '/batch' -Count 10
        $result.records | Group-Object partition | Sort-Object Name |
            Format-Table @{Label='Partition'; Expression={$_.Name}}, Count -AutoSize
    }
} catch {
    [Console]::Error.WriteLine("Demo failed: $($_.Exception.Message)")
    if ($_.ErrorDetails.Message) {
        [Console]::Error.WriteLine($_.ErrorDetails.Message)
    }
    exit 1
}
