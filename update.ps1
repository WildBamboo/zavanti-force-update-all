param (
    [string] $sqlConnectionString,
    [string] $messageBusConnectionString,
    [Parameter(Mandatory=$True)]
    [ValidateSet('PrepareOnly', 'ProcessOnly', 'PrepareAndProcess')]
    [string] $mode,
    [string] $queueName = "zavanti_update",
    [string] $orgName = "linkpeople"
)


if ($PSVersionTable.PSVersion.Major -lt 7 ) {
    Write-Host("Powershell 7 or greater must be used")
    exit
}

enum Mode { 
    PrepareOnly = 1;
    ProcessOnly = 2;
    PrepareAndProcess = 3;
}

[Mode]$mode = $mode

Write-Output "Zavanti force update"


# Constants
$pendingPath = "pending"
$completePath = "complete"

function Invoke-SQL {
    param(
        [string] $sqlCommand = $(throw "Please specify a query.")
    )

    $connection = new-object system.data.SqlClient.SQLConnection($sqlConnectionString)
    $command = new-object system.data.sqlclient.sqlcommand($sqlCommand,$connection)
    $connection.Open()
    
    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataSet) | Out-Null
    
    $connection.Close()
    $dataSet.Tables
}

function Get-AllPersonIds {
    Invoke-SQL -sqlCommand "SELECT [Id] FROM [dbo].[Person] WHERE [deletedDate] IS NULL" `
        % {$_.Id} 
}

class ContactWithId {
    [int] $PersonId
    [System.Data.DataRow] $Row

    ContactWithId([int] $id, [System.Data.DataRow] $row) {
        $this.PersonId = $id
        $this.Row = $row
    }
}

function Get-Contact {
    param (
        [int] $personId
    )

    $query = "EXECUTE [dbo].[GetZavantiContact] @PersonId = $personId" 
    $contact = Invoke-SQL -connectionString $conn -sqlCommand $query

    if ($contact.Rows.Count -eq 1) {
        $row = $contact.Rows[0]
        return [ContactWithId]::new($personId, $row)
    }
    elseif ($contact.Rows.Count -gt 1) {
        Write-Error "Multiple contact records returned for $personId"
    }
}


function ZavantiContractToJson {
    param (
        [ContactWithId] $contactWithId
    )

    $personRecord = $contactWithId.Row

    return (ConvertTo-Json @{
        OrganisationName = $orgName
        Contact = @{
            PersonId = $contactWithId.PersonId
            
            FirstName = $personRecord.FirstName
            LastName = $personRecord.LastName
            PreferredName = $personRecord.PreferredName
            DateOfBirth = $personRecord.DateOfBirth
            EthnicityName = $personRecord.PrimaryEthnicityText
            
            Gender = @{
                Code = $personRecord.GenderValue
                DisplayName = $personRecord.GenderName
            }
            
            Phone = @{
                PhoneNumber1 = $personRecord.PhoneNumber1
                PhoneNumber2 = $personRecord.PhoneNumber2
                Mobile = $personRecord.MobilePhoneNumber
            }

            ReferenceNumbers = @{
                NHI = $personRecord.NhiNumber
                WINZ = $personRecord.WinzNumber
                SocialHousingRating = $personRecord.SocialHousingRating
            }

            ServiceLevel = $personRecord.SupportLevel
            CaseworkerEmailAddress = $personRecord.CaseworkerEmailAddress

            EmergencyContact1 = @{ 
                Name = $personRecord.EmergencyContactName1
                RelationshipType = $personRecord.EmergencyContactRelationship1
                PhoneNumber = $personRecord.EmergencyContactPhone1
                Email = $personRecord.EmergencyContactEmail1
            }

            EmergencyContact2 = @{
                Name = $personRecord.EmergencyContactName2
                RelationshipType = $personRecord.EmergencyContactRelationship2
                PhoneNumber = $personRecord.EmergencyContactPhone2
                Email = $personRecord.EmergencyContactEmail2
            }
        }
    })
}

function SaveMessageToDisk {
    param (
        $json
    )

    $tmp = New-TemporaryFile
    $json > $tmp
    return $tmp
}


function CreateBatch {
    New-Item -ItemType Directory -Force -Name $pendingPath | Out-Null

    $count = 0
    foreach ($id in (Get-AllPersonIds)) {
        New-Item -ItemType File -Path ".\$pendingPath" -Name $id.Id -Force | Out-Null
        $count += 1
    }
    Write-Output "Created $count pending people for migration" 
}

function ProcessBatch {
    New-Item -ItemType Directory -Force -Name $completePath | Out-Null

    foreach ($path in Get-ChildItem $pendingPath) {
        $id = [int]$path.BaseName
        $contact = Get-Contact $id

        if ($contact) {
            Write-Output "Sending update for $id"
            $messageFile = SaveMessageToDisk(ZavantiContractToJson($contact))            
            .\postjson\PostJson.exe --bus $messageBusConnectionString --queue $queueName --file $messageFile.FullName
            Remove-Item $messageFile     
        }
        
        Move-Item -Path $path -Destination $completePath -Force
    }
}

if ($mode -band [Mode]::PrepareOnly) {
    CreateBatch
}
if ($mode -band [Mode]::ProcessOnly) {
    ProcessBatch
}