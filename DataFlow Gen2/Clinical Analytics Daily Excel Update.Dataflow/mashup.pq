[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Team_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Reporting Day of Week", DestinationColumnName = "Reporting Day of Week"], [SourceColumnName = "Hour", DestinationColumnName = "Hour"], [SourceColumnName = "Organization", DestinationColumnName = "Organization"], [SourceColumnName = "Team Code", DestinationColumnName = "Team Code"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Team = let
    Source = SharePoint.Files("https://intellatriage.sharepoint.com/sites/PowerBIData", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each ([Name] <> "IntellaHub Call Export.xlsx")),
    #"Teams xlsx_https://intellatriage sharepoint com/sites/PowerBIData/Shared Documents/Reference Tables/" = #"Filtered Rows"{[Name="Teams.xlsx",#"Folder Path"="https://intellatriage.sharepoint.com/sites/PowerBIData/Shared Documents/Reference Tables/"]}[Content],
    #"Imported Excel Workbook" = Excel.Workbook(#"Teams xlsx_https://intellatriage sharepoint com/sites/PowerBIData/Shared Documents/Reference Tables/"),
    TeamTable_Table = #"Imported Excel Workbook"{[Item="TeamTable",Kind="Table"]}[Data],
    #"Unpivoted Other Columns" = Table.UnpivotOtherColumns(TeamTable_Table, {"Hour", "Reporting Day of Week"}, "Attribute", "Value"),
    #"Renamed Columns" = Table.RenameColumns(#"Unpivoted Other Columns",{{"Attribute", "Organization"}, {"Value", "Team Code"}})
in
    #"Renamed Columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Procedure_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Client", DestinationColumnName = "Client"], [SourceColumnName = "Specialty", DestinationColumnName = "Specialty"], [SourceColumnName = "Subcategory", DestinationColumnName = "Subcategory"], [SourceColumnName = "Expected Result", DestinationColumnName = "Expected Result"], [SourceColumnName = "ClientSpecialtySubcategory", DestinationColumnName = "ClientSpecialtySubcategory"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Procedure = let
    Source = SharePoint.Files("https://intellatriage.sharepoint.com/sites/PowerBIData", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each ([Name] <> "IntellaHub Call Export.xlsx")),
    #"Procedure xlsx_https://intellatriage sharepoint com/sites/PowerBIData/Shared Documents/Reference Tables/" = #"Filtered Rows"{[Name="Procedure.xlsx",#"Folder Path"="https://intellatriage.sharepoint.com/sites/PowerBIData/Shared Documents/Reference Tables/"]}[Content],
    #"Imported Excel Workbook" = Excel.Workbook(#"Procedure xlsx_https://intellatriage sharepoint com/sites/PowerBIData/Shared Documents/Reference Tables/"),
    Table1115_Table = #"Imported Excel Workbook"{[Item="Table1115",Kind="Table"]}[Data],
    #"Unpivoted Other Columns" = Table.UnpivotOtherColumns(Table1115_Table, {"Client", "Specialty"}, "Attribute", "Value"),
    #"Renamed Columns" = Table.RenameColumns(#"Unpivoted Other Columns",{{"Attribute", "Subcategory"}, {"Value", "Expected Result"}}),
    #"Added Custom" = Table.AddColumn(#"Renamed Columns", "ClientSpecialtySubcategory", each [Client]&" "&[Specialty]&" "&[Subcategory]),
    #"Changed column type" = Table.TransformColumnTypes(#"Added Custom", {{"Expected Result", type text}, {"ClientSpecialtySubcategory", type text}}),
    
    // ADD CLEAN COLUMNS - Fixed to reference correct step name
    #"Added Client_Clean" = Table.AddColumn(#"Changed column type", "Client_Clean", each 
        try Text.Upper(Text.Trim([Client])) otherwise null,
        type text
    ),
    
    #"Added Specialty_Clean" = Table.AddColumn(#"Added Client_Clean", "Specialty_Clean", each 
        try Text.Upper(Text.Trim([Specialty])) otherwise null,
        type text
    ),
    
    #"Added Subcategory_Clean" = Table.AddColumn(#"Added Specialty_Clean", "Subcategory_Clean", each 
        try Text.Upper(Text.Trim([Subcategory])) otherwise null,
        type text
    )
in
    #"Added Subcategory_Clean";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Action_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Action", DestinationColumnName = "Action"], [SourceColumnName = "Resolution", DestinationColumnName = "Resolution"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Action = let
    Source = SharePoint.Files("https://intellatriage.sharepoint.com/sites/PowerBIData", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each ([Name] <> "IntellaHub Call Export.xlsx")),
    #"Action xlsx_https://intellatriage sharepoint com/sites/PowerBIData/Shared Documents/Reference Tables/" = #"Filtered Rows"{[Name="Action.xlsx",#"Folder Path"="https://intellatriage.sharepoint.com/sites/PowerBIData/Shared Documents/Reference Tables/"]}[Content],
    #"Imported Excel Workbook" = Excel.Workbook(#"Action xlsx_https://intellatriage sharepoint com/sites/PowerBIData/Shared Documents/Reference Tables/"),
    ActionsID_Table = #"Imported Excel Workbook"{[Item="ActionsID",Kind="Table"]}[Data],
    #"Removed Duplicates" = Table.Distinct(ActionsID_Table, {"Action"})
in
    #"Removed Duplicates";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Nature_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Nature", DestinationColumnName = "Nature"], [SourceColumnName = "DAR Category", DestinationColumnName = "DAR Category"], [SourceColumnName = "Category", DestinationColumnName = "Category"], [SourceColumnName = "Protocol Notification", DestinationColumnName = "Protocol Notification"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Nature = let
    Source = SharePoint.Files("https://intellatriage.sharepoint.com/sites/PowerBIData", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each ([Name] <> "IntellaHub Call Export.xlsx")),
    #"Nature xlsx_https://intellatriage sharepoint com/sites/PowerBIData/Shared Documents/Reference Tables/" = #"Filtered Rows"{[Name="Nature.xlsx",#"Folder Path"="https://intellatriage.sharepoint.com/sites/PowerBIData/Shared Documents/Reference Tables/"]}[Content],
    #"Imported Excel Workbook" = Excel.Workbook(#"Nature xlsx_https://intellatriage sharepoint com/sites/PowerBIData/Shared Documents/Reference Tables/"),
    NatureID_Table = #"Imported Excel Workbook"{[Item="NatureID",Kind="Table"]}[Data],
    #"Removed Duplicates" = Table.Distinct(NatureID_Table, {"Nature"})
in
    #"Removed Duplicates";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Customer_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Customer", DestinationColumnName = "Customer"], [SourceColumnName = "Program", DestinationColumnName = "Program"], [SourceColumnName = "Specialty", DestinationColumnName = "Specialty"], [SourceColumnName = "Combo", DestinationColumnName = "Combo"], [SourceColumnName = "BAYADA Team", DestinationColumnName = "BAYADA Team"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Customer = let
    Source = SharePoint.Files("https://intellatriage.sharepoint.com/sites/PowerBIData", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each ([Name] <> "IntellaHub Call Export.xlsx")),
    #"Customer xlsx_https://intellatriage sharepoint com/sites/PowerBIData/Shared Documents/Reference Tables/" = #"Filtered Rows"{[Name="Customer.xlsx",#"Folder Path"="https://intellatriage.sharepoint.com/sites/PowerBIData/Shared Documents/Reference Tables/"]}[Content],
    #"Imported Excel Workbook" = Excel.Workbook(#"Customer xlsx_https://intellatriage sharepoint com/sites/PowerBIData/Shared Documents/Reference Tables/"),
    Sheet1_Sheet = #"Imported Excel Workbook"{[Item="Sheet1",Kind="Sheet"]}[Data],
    #"Promoted Headers" = Table.PromoteHeaders(Sheet1_Sheet, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Customer", type text}, {"Program", type text}, {"Specialty", type text}, {"Combo", type text}, {"BAYADA Team", type text}}),
    #"Removed Duplicates" = Table.Distinct(#"Changed Type", {"Program"}),
    #"Filtered Rows1" = Table.SelectRows(#"Removed Duplicates", each [Program] <> null and [Program] <> "")
in
    #"Filtered Rows1";
shared #"IntellaHub Export Data 2023_2024" = let
  Source = Fabric.Warehouse(null),
  Navigation = Source{[workspaceId = "6f688f9a-787a-46c6-8068-8d6e88edd051"]}[Data],
  #"Navigation 1" = Navigation{[warehouseId = "cfa259ab-5ffa-439c-baa6-8b58678a13de"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Schema = "dbo", Item = "IntellaHub Export Data 2023_2024"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "IntellaHub Export Data_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "ClientCallUID", DestinationColumnName = "ClientCallUID"], [SourceColumnName = "CreatedOn", DestinationColumnName = "CreatedOn"], [SourceColumnName = "ModifiedOn", DestinationColumnName = "ModifiedOn"], [SourceColumnName = "CreatedBy", DestinationColumnName = "CreatedBy"], [SourceColumnName = "TimeZone", DestinationColumnName = "TimeZone"], [SourceColumnName = "ModifiedBy", DestinationColumnName = "ModifiedBy"], [SourceColumnName = "Date", DestinationColumnName = "Date"], [SourceColumnName = "Time", DestinationColumnName = "Time"], [SourceColumnName = "CallerName", DestinationColumnName = "CallerName"], [SourceColumnName = "PatientName", DestinationColumnName = "PatientName"], [SourceColumnName = "ClientField1", DestinationColumnName = "ClientField1"], [SourceColumnName = "ClientField2", DestinationColumnName = "ClientField2"], [SourceColumnName = "ClientField3", DestinationColumnName = "ClientField3"], [SourceColumnName = "ClientField4", DestinationColumnName = "ClientField4"], [SourceColumnName = "ClientField5", DestinationColumnName = "ClientField5"], [SourceColumnName = "ClientName", DestinationColumnName = "ClientName"], [SourceColumnName = "ProgramName", DestinationColumnName = "ProgramName"], [SourceColumnName = "RelationshipName", DestinationColumnName = "RelationshipName"], [SourceColumnName = "NatureName", DestinationColumnName = "NatureName"], [SourceColumnName = "ActionsName", DestinationColumnName = "ActionsName"], [SourceColumnName = "DateOfBirth", DestinationColumnName = "DateOfBirth"], [SourceColumnName = "CallBackNumber", DestinationColumnName = "CallBackNumber"], [SourceColumnName = "PublishedBy", DestinationColumnName = "PublishedBy"], [SourceColumnName = "PublishedOn", DestinationColumnName = "PublishedOn"], [SourceColumnName = "ClientProgramCombo", DestinationColumnName = "ClientProgramCombo"], [SourceColumnName = "Day", DestinationColumnName = "Day"], [SourceColumnName = "Hour", DestinationColumnName = "Hour"], [SourceColumnName = "Team.Team Code", DestinationColumnName = "Team.Team Code"], [SourceColumnName = "ActionsName_Clean", DestinationColumnName = "ActionsName_Clean"], [SourceColumnName = "ModifiedOnClean", DestinationColumnName = "ModifiedOnClean"], [SourceColumnName = "Specialty_FromCustomer", DestinationColumnName = "Specialty_FromCustomer"], [SourceColumnName = "Weekday", DestinationColumnName = "Weekday"], [SourceColumnName = "Day of Week", DestinationColumnName = "Day of Week"], [SourceColumnName = "24x7 Breakout", DestinationColumnName = "24x7 Breakout"], [SourceColumnName = "Expected Result", DestinationColumnName = "Expected Result"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"IntellaHub Export Data" = let
    // Load 2023 data
    Source = SharePoint.Files("https://intellatriage.sharepoint.com/sites/PowerBIData", [ApiVersion = 15]),
    #"2025File" = 
        Source{[Name="IntellaHub Call Export.xlsx",#"Folder Path"="https://intellatriage.sharepoint.com/sites/PowerBIData/Shared Documents/Data Exports/"]}[Content],
    #"Imported Excel Workbook" = Excel.Workbook(#"2025File"),
    IntellaHubDataTable_Table = #"Imported Excel Workbook"{[Item="IntellaHubDataTable",Kind="Table"]}[Data],

     
    // NOW do all transformations ONCE on the combined dataset
#"Replaced All Errors" = Table.ReplaceErrorValues(IntellaHubDataTable_Table, 
        List.Transform(
            Table.ColumnNames(IntellaHubDataTable_Table),
            each {_, null}
        )
    ),
    
    // CRITICAL FIX 2: Safe type conversions with try/otherwise for dates and numbers
    #"Convert CreatedOn" = Table.TransformColumns(#"Replaced All Errors", {
        {"CreatedOn", each try DateTime.From(_) otherwise null, type datetime}
    }),
    #"Convert ModifiedOn" = Table.TransformColumns(#"Convert CreatedOn", {
        {"ModifiedOn", each try DateTime.From(_) otherwise null, type datetime}
    }),
    #"Convert Date" = Table.TransformColumns(#"Convert ModifiedOn", {
        {"Date", each try Date.From(_) otherwise null, type date}
    }),
    #"Convert Time" = Table.TransformColumns(#"Convert Date", {
        {"Time", each try DateTime.From(_) otherwise null, type datetime}
    }),
    #"Convert DateOfBirth" = Table.TransformColumns(#"Convert Time", {
        {"DateOfBirth", each try Date.From(_) otherwise null, type date}
    }),
    #"Convert PublishedOn" = Table.TransformColumns(#"Convert DateOfBirth", {
        {"PublishedOn", each try DateTime.From(_) otherwise null, type datetime}
    }),
    #"Convert ClientCallUID" = Table.TransformColumns(#"Convert PublishedOn", {
        {"ClientCallUID", each try Text.From(_) otherwise null, type text}
    }),
    
    // CRITICAL FIX 3: Safe custom columns with error handling
    #"Added Custom" = Table.AddColumn(#"Convert ClientCallUID", "ClientProgramCombo", each 
        try Text.From([ClientName]) & " " & Text.From([ProgramName]) otherwise null
    ),
    #"Added Custom1" = Table.AddColumn(#"Added Custom", "Day of Week", each 
        try Date.DayOfWeek(Date.From([Date]), Day.Monday) + 1 otherwise null
    ),
    #"Changed Type1" = Table.TransformColumnTypes(#"Added Custom1",{{"Date", type datetime}}),
    #"Added Custom2" = Table.AddColumn(#"Changed Type1", "Hour", each 
        try Time.Hour([Time]) otherwise null
    ),
    
    // Merge with Team table
    #"Merged Queries" = Table.NestedJoin(#"Added Custom2", {"Day of Week", "Hour", "ClientName"}, Team, {"Reporting Day of Week", "Hour", "Organization"}, "Team", JoinKind.LeftOuter),
    #"Expanded Team" = Table.ExpandTableColumn(#"Merged Queries", "Team", {"Team Code"}, {"Team.Team Code"}),
    #"Replaced Value" = Table.ReplaceValue(#"Expanded Team", null, "Outbound", Replacer.ReplaceValue, {"Team.Team Code"}),
    
    // Safe type conversions for calculated columns
    #"Changed Type2" = Table.TransformColumns(#"Replaced Value", {
        {"Day of Week", each try Int64.From(_) otherwise null, Int64.Type},
        {"Hour", each try Int64.From(_) otherwise null, Int64.Type}
    }),
    
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type2", {{"Day of Week", "Day"}}),
    #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns",{"Comments"}),
    
    // Replace errors in text columns
    #"Replaced Errors" = Table.ReplaceErrorValues(#"Removed Columns", {{"PatientName", ""}}),
    #"Replaced Errors1" = Table.ReplaceErrorValues(#"Replaced Errors", {{"CallerName", ""}}),
    
    // Safe text transformation
    #"Added Custom3" = Table.AddColumn(#"Replaced Errors1", "ActionsName_Clean", each 
        try (
            if Text.StartsWith([ActionsName], "Behavioral Health") then
                [ActionsName]
            else if Text.Contains([ActionsName], " -") then
                Text.BeforeDelimiter([ActionsName], " -", 0)
            else
                [ActionsName]
        ) otherwise null
    ),
    
    #"Added Custom4" = Table.AddColumn(#"Added Custom3", "ModifiedOnClean", each 
        if [ModifiedOn] = null then [CreatedOn] else [ModifiedOn]
    ),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom4", {{"ModifiedOnClean", type datetime}, {"ActionsName_Clean", type text}, {"CallBackNumber", type text}, {"ClientProgramCombo", type text}, {"ClientField4", type text}, {"ClientField3", type text}, {"ClientField2", type text}, {"ClientField1", type text}, {"PatientName", type text}, {"CallerName", type text}}),
    
    // ⭐ ADD CUSTOMER LOOKUP HERE (APPLIES TO ALL DATA)
    #"Merged with Customer" = Table.NestedJoin(
        #"Changed Type3", 
        {"ProgramName"}, 
        Customer, 
        {"Program"}, 
        "Customer", 
        JoinKind.LeftOuter
    ),
    
    #"Expanded Specialty" = Table.ExpandTableColumn(
        #"Merged with Customer", 
        "Customer", 
        {"Specialty"}, 
        {"Specialty_FromCustomer"}
    ),

    // ADD NEW COLUMNS
    #"Added Weekday" = Table.AddColumn(#"Expanded Specialty", "Weekday", each 
        try DateTime.ToText([Date], "ddd") otherwise null,
        type text
    ),
    
    #"Added Day of Week" = Table.AddColumn(#"Added Weekday", "Day of Week", each 
        try (
            if Date.DayOfWeek([CreatedOn], Day.Monday) >= 5 then
                "WEEKEND"
            else
                "WEEKDAY"
        ) otherwise null,
        type text
    ),
    
    #"Added 24x7 Breakout" = Table.AddColumn(#"Added Day of Week", "24x7 Breakout", each 
        try (
            if [Day] >= 1 and [Day] <= 5 and [Hour] > 7 and [Hour] < 17 then
                "Business Hours"
            else
                "After Hours"
        ) otherwise null,
        type text
    ),
    
    // Expected Result lookup
    #"Added CleanClient" = Table.AddColumn(#"Added 24x7 Breakout", "ClientName_Clean", each 
        Text.Upper(Text.Trim([ClientName])), type text
    ),
    #"Added CleanSpecialty" = Table.AddColumn(#"Added CleanClient", "Specialty_Clean", each 
        try Text.Upper(Text.Trim([Specialty_FromCustomer])) otherwise null, type text
    ),
    #"Added CleanNature" = Table.AddColumn(#"Added CleanSpecialty", "NatureName_Clean", each 
        Text.Upper(Text.Trim([NatureName])), type text
    ),
    
    #"Merged with Procedure" = Table.NestedJoin(
        #"Added CleanNature", 
        {"ClientName_Clean", "Specialty_Clean", "NatureName_Clean"}, 
        Procedure, 
        {"Client_Clean", "Specialty_Clean", "Subcategory_Clean"},
        "Procedure", 
        JoinKind.LeftOuter
    ),
    
    #"Expanded Expected Result" = Table.ExpandTableColumn(
        #"Merged with Procedure", 
        "Procedure", 
        {"Expected Result"}, 
        {"Expected Result"}
    ),

    // Remove temporary clean columns used for lookup
    #"Removed Clean Columns" = Table.RemoveColumns(#"Expanded Expected Result", 
        {"ClientName_Clean", "Specialty_Clean", "NatureName_Clean"}
    ),

    // Final type check for all new columns
    #"Final Type Check" = Table.TransformColumnTypes(#"Removed Clean Columns", {{"Expected Result", type text}, {"Weekday", type text}, {"Day of Week", type text}, {"24x7 Breakout", type text}, {"Specialty_FromCustomer", type text}, {"CreatedBy", type text}, {"ClientField5", type text}, {"TimeZone", type text}, {"ActionsName", type text}, {"PublishedBy", type text}}),
    
// Align the 2023_2024 table columns with main dataset
Aligned_2023_2024 =
    Table.SelectColumns(
        #"IntellaHub Export Data 2023_2024",
        Table.ColumnNames(#"Final Type Check"),
        MissingField.UseNull
    ),

// Append the warehouse data
#"Appended Main + 2023_2024" =
    Table.Combine({#"Final Type Check", Aligned_2023_2024})

in
    #"Appended Main + 2023_2024";
shared Team_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6f688f9a-787a-46c6-8068-8d6e88edd051"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "cfa259ab-5ffa-439c-baa6-8b58678a13de"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "Team"]}[Data]
in
  TableNavigation;
shared Procedure_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6f688f9a-787a-46c6-8068-8d6e88edd051"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "cfa259ab-5ffa-439c-baa6-8b58678a13de"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "Procedure"]}[Data]
in
  TableNavigation;
shared Action_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6f688f9a-787a-46c6-8068-8d6e88edd051"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "cfa259ab-5ffa-439c-baa6-8b58678a13de"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "Action"]}[Data]
in
  TableNavigation;
shared Nature_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6f688f9a-787a-46c6-8068-8d6e88edd051"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "cfa259ab-5ffa-439c-baa6-8b58678a13de"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "Nature"]}[Data]
in
  TableNavigation;
shared Customer_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6f688f9a-787a-46c6-8068-8d6e88edd051"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "cfa259ab-5ffa-439c-baa6-8b58678a13de"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "Customer"]}[Data]
in
  TableNavigation;
shared #"IntellaHub Export Data_DataDestination" = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6f688f9a-787a-46c6-8068-8d6e88edd051"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "cfa259ab-5ffa-439c-baa6-8b58678a13de"]}[Data],
  TableNavigation = Navigation_2{[Item = "IntellaHub Export Data", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
