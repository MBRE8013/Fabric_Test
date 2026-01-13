[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Date", DestinationColumnName = "Date"], [SourceColumnName = "DateKey", DestinationColumnName = "DateKey"], [SourceColumnName = "Year", DestinationColumnName = "Year"], [SourceColumnName = "YearShort", DestinationColumnName = "YearShort"], [SourceColumnName = "YearShortSort", DestinationColumnName = "YearShortSort"], [SourceColumnName = "QuarterNumber", DestinationColumnName = "QuarterNumber"], [SourceColumnName = "Quarter", DestinationColumnName = "Quarter"], [SourceColumnName = "QuarterSort", DestinationColumnName = "QuarterSort"], [SourceColumnName = "YearQuarter", DestinationColumnName = "YearQuarter"], [SourceColumnName = "YearQuarterSort", DestinationColumnName = "YearQuarterSort"], [SourceColumnName = "YearQuarterShort", DestinationColumnName = "YearQuarterShort"], [SourceColumnName = "YearQuarterShortSort", DestinationColumnName = "YearQuarterShortSort"], [SourceColumnName = "MonthNumber", DestinationColumnName = "MonthNumber"], [SourceColumnName = "MonthName", DestinationColumnName = "MonthName"], [SourceColumnName = "MonthNameSort", DestinationColumnName = "MonthNameSort"], [SourceColumnName = "MonthNameShort", DestinationColumnName = "MonthNameShort"], [SourceColumnName = "MonthNameShortSort", DestinationColumnName = "MonthNameShortSort"], [SourceColumnName = "YearMonth", DestinationColumnName = "YearMonth"], [SourceColumnName = "YearMonthSort", DestinationColumnName = "YearMonthSort"], [SourceColumnName = "YearMonthShort", DestinationColumnName = "YearMonthShort"], [SourceColumnName = "YearMonthShortSort", DestinationColumnName = "YearMonthShortSort"], [SourceColumnName = "YearMonthName", DestinationColumnName = "YearMonthName"], [SourceColumnName = "YearMonthNameSort", DestinationColumnName = "YearMonthNameSort"], [SourceColumnName = "YearMonthNameShort", DestinationColumnName = "YearMonthNameShort"], [SourceColumnName = "YearMonthNameShortSort", DestinationColumnName = "YearMonthNameShortSort"], [SourceColumnName = "MonthYearName", DestinationColumnName = "MonthYearName"], [SourceColumnName = "MonthYearNameSort", DestinationColumnName = "MonthYearNameSort"], [SourceColumnName = "MonthYearNameShort", DestinationColumnName = "MonthYearNameShort"], [SourceColumnName = "MonthYearNameShortSort", DestinationColumnName = "MonthYearNameShortSort"], [SourceColumnName = "WeekOfYear", DestinationColumnName = "WeekOfYear"], [SourceColumnName = "WeekOfMonth", DestinationColumnName = "WeekOfMonth"], [SourceColumnName = "DayNumber", DestinationColumnName = "DayNumber"], [SourceColumnName = "DayOfWeekNumber", DestinationColumnName = "DayOfWeekNumber"], [SourceColumnName = "DayOfWeekName", DestinationColumnName = "DayOfWeekName"], [SourceColumnName = "DayOfWeekNameSort", DestinationColumnName = "DayOfWeekNameSort"], [SourceColumnName = "DayOfWeekNameShort", DestinationColumnName = "DayOfWeekNameShort"], [SourceColumnName = "DayOfWeekNameShortSort", DestinationColumnName = "DayOfWeekNameShortSort"], [SourceColumnName = "DayOfWeekAbbr", DestinationColumnName = "DayOfWeekAbbr"], [SourceColumnName = "DayOfWeekAbbrSort", DestinationColumnName = "DayOfWeekAbbrSort"], [SourceColumnName = "DayOfYear", DestinationColumnName = "DayOfYear"], [SourceColumnName = "FiscalYear", DestinationColumnName = "FiscalYear"], [SourceColumnName = "FiscalQuarter", DestinationColumnName = "FiscalQuarter"], [SourceColumnName = "FiscalQuarterName", DestinationColumnName = "FiscalQuarterName"], [SourceColumnName = "FiscalQuarterNameSort", DestinationColumnName = "FiscalQuarterNameSort"], [SourceColumnName = "FiscalMonth", DestinationColumnName = "FiscalMonth"], [SourceColumnName = "IsWeekend", DestinationColumnName = "IsWeekend"], [SourceColumnName = "IsWeekday", DestinationColumnName = "IsWeekday"], [SourceColumnName = "IsCurrentMonth", DestinationColumnName = "IsCurrentMonth"], [SourceColumnName = "IsCurrentQuarter", DestinationColumnName = "IsCurrentQuarter"], [SourceColumnName = "IsCurrentYear", DestinationColumnName = "IsCurrentYear"], [SourceColumnName = "MonthOffset", DestinationColumnName = "MonthOffset"], [SourceColumnName = "QuarterOffset", DestinationColumnName = "QuarterOffset"], [SourceColumnName = "YearOffset", DestinationColumnName = "YearOffset"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Query = let
    // Configuration
    StartDate = #datetime(2020, 1, 1, 0, 0, 0),
    EndDate = #datetime(2030, 12, 31, 23, 59, 59),
    FiscalYearStartMonth = 7,  // July = fiscal year start (adjust as needed)
    
    // Generate date list
    NumberOfDays = Duration.Days(EndDate - StartDate) + 1,
    DateList = List.Dates(Date.From(StartDate), NumberOfDays, #duration(1,0,0,0)),
    #"Converted to Table" = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}),
    #"Changed Type to DateTime" = Table.TransformColumnTypes(#"Converted to Table",{{"Date", type datetime}}),
    
    // Date Key (YYYYMMDD format as integer)
    #"Added DateKey" = Table.AddColumn(#"Changed Type to DateTime", "DateKey", each Date.Year(DateTime.Date([Date])) * 10000 + Date.Month(DateTime.Date([Date])) * 100 + Date.Day(DateTime.Date([Date])), Int64.Type),
    
    // Year columns
    #"Added Year" = Table.AddColumn(#"Added DateKey", "Year", each Date.Year(DateTime.Date([Date])), Int64.Type),
    #"Added YearShort" = Table.AddColumn(#"Added Year", "YearShort", each Text.End(Text.From(Date.Year(DateTime.Date([Date]))), 2), type text),
    #"Added YearShortSort" = Table.AddColumn(#"Added YearShort", "YearShortSort", each Date.Year(DateTime.Date([Date])), Int64.Type),
    
    // Quarter columns
    #"Added QuarterNumber" = Table.AddColumn(#"Added YearShortSort", "QuarterNumber", each Date.QuarterOfYear(DateTime.Date([Date])), Int64.Type),
    #"Added Quarter" = Table.AddColumn(#"Added QuarterNumber", "Quarter", each "Q" & Text.From(Date.QuarterOfYear(DateTime.Date([Date]))), type text),
    #"Added QuarterSort" = Table.AddColumn(#"Added Quarter", "QuarterSort", each Date.QuarterOfYear(DateTime.Date([Date])), Int64.Type),
    #"Added YearQuarter" = Table.AddColumn(#"Added QuarterSort", "YearQuarter", each Text.From(Date.Year(DateTime.Date([Date]))) & " Q" & Text.From(Date.QuarterOfYear(DateTime.Date([Date]))), type text),
    #"Added YearQuarterSort" = Table.AddColumn(#"Added YearQuarter", "YearQuarterSort", each Date.Year(DateTime.Date([Date])) * 10 + Date.QuarterOfYear(DateTime.Date([Date])), Int64.Type),
    #"Added YearQuarterShort" = Table.AddColumn(#"Added YearQuarterSort", "YearQuarterShort", each Text.End(Text.From(Date.Year(DateTime.Date([Date]))), 2) & "Q" & Text.From(Date.QuarterOfYear(DateTime.Date([Date]))), type text),
    #"Added YearQuarterShortSort" = Table.AddColumn(#"Added YearQuarterShort", "YearQuarterShortSort", each Date.Year(DateTime.Date([Date])) * 10 + Date.QuarterOfYear(DateTime.Date([Date])), Int64.Type),
    
    // Month columns
    #"Added MonthNumber" = Table.AddColumn(#"Added YearQuarterShortSort", "MonthNumber", each Date.Month(DateTime.Date([Date])), Int64.Type),
    #"Added MonthName" = Table.AddColumn(#"Added MonthNumber", "MonthName", each Date.MonthName(DateTime.Date([Date])), type text),
    #"Added MonthNameSort" = Table.AddColumn(#"Added MonthName", "MonthNameSort", each Date.Month(DateTime.Date([Date])), Int64.Type),
    #"Added MonthNameShort" = Table.AddColumn(#"Added MonthNameSort", "MonthNameShort", each Text.Start(Date.MonthName(DateTime.Date([Date])), 3), type text),
    #"Added MonthNameShortSort" = Table.AddColumn(#"Added MonthNameShort", "MonthNameShortSort", each Date.Month(DateTime.Date([Date])), Int64.Type),
    #"Added YearMonth" = Table.AddColumn(#"Added MonthNameShortSort", "YearMonth", each Text.From(Date.Year(DateTime.Date([Date]))) & "-" & Text.PadStart(Text.From(Date.Month(DateTime.Date([Date]))), 2, "0"), type text),
    #"Added YearMonthSort" = Table.AddColumn(#"Added YearMonth", "YearMonthSort", each Date.Year(DateTime.Date([Date])) * 100 + Date.Month(DateTime.Date([Date])), Int64.Type),
    #"Added YearMonthShort" = Table.AddColumn(#"Added YearMonthSort", "YearMonthShort", each Text.End(Text.From(Date.Year(DateTime.Date([Date]))), 2) & "-" & Text.PadStart(Text.From(Date.Month(DateTime.Date([Date]))), 2, "0"), type text),
    #"Added YearMonthShortSort" = Table.AddColumn(#"Added YearMonthShort", "YearMonthShortSort", each Date.Year(DateTime.Date([Date])) * 100 + Date.Month(DateTime.Date([Date])), Int64.Type),
    #"Added YearMonthName" = Table.AddColumn(#"Added YearMonthShortSort", "YearMonthName", each Text.From(Date.Year(DateTime.Date([Date]))) & " " & Date.MonthName(DateTime.Date([Date])), type text),
    #"Added YearMonthNameSort" = Table.AddColumn(#"Added YearMonthName", "YearMonthNameSort", each Date.Year(DateTime.Date([Date])) * 100 + Date.Month(DateTime.Date([Date])), Int64.Type),
    #"Added YearMonthNameShort" = Table.AddColumn(#"Added YearMonthNameSort", "YearMonthNameShort", each Text.End(Text.From(Date.Year(DateTime.Date([Date]))), 2) & " " & Text.Start(Date.MonthName(DateTime.Date([Date])), 3), type text),
    #"Added YearMonthNameShortSort" = Table.AddColumn(#"Added YearMonthNameShort", "YearMonthNameShortSort", each Date.Year(DateTime.Date([Date])) * 100 + Date.Month(DateTime.Date([Date])), Int64.Type),
    #"Added MonthYearName" = Table.AddColumn(#"Added YearMonthNameShortSort", "MonthYearName", each Date.MonthName(DateTime.Date([Date])) & " " & Text.From(Date.Year(DateTime.Date([Date]))), type text),
    #"Added MonthYearNameSort" = Table.AddColumn(#"Added MonthYearName", "MonthYearNameSort", each Date.Year(DateTime.Date([Date])) * 100 + Date.Month(DateTime.Date([Date])), Int64.Type),
    #"Added MonthYearNameShort" = Table.AddColumn(#"Added MonthYearNameSort", "MonthYearNameShort", each Text.Start(Date.MonthName(DateTime.Date([Date])), 3) & " " & Text.End(Text.From(Date.Year(DateTime.Date([Date]))), 2), type text),
    #"Added MonthYearNameShortSort" = Table.AddColumn(#"Added MonthYearNameShort", "MonthYearNameShortSort", each Date.Year(DateTime.Date([Date])) * 100 + Date.Month(DateTime.Date([Date])), Int64.Type),
    
    // Week columns
    #"Added WeekOfYear" = Table.AddColumn(#"Added MonthYearNameShortSort", "WeekOfYear", each Date.WeekOfYear(DateTime.Date([Date]), Day.Monday), Int64.Type),
    #"Added WeekOfMonth" = Table.AddColumn(#"Added WeekOfYear", "WeekOfMonth", each Number.RoundUp((Date.Day(DateTime.Date([Date])) + Date.DayOfWeek(Date.StartOfMonth(DateTime.Date([Date])), Day.Monday)) / 7), Int64.Type),
    
    // Day columns
    #"Added DayNumber" = Table.AddColumn(#"Added WeekOfMonth", "DayNumber", each Date.Day(DateTime.Date([Date])), Int64.Type),
    #"Added DayOfWeekNumber" = Table.AddColumn(#"Added DayNumber", "DayOfWeekNumber", each Date.DayOfWeek(DateTime.Date([Date]), Day.Monday) + 1, Int64.Type),
    #"Added DayOfWeekName" = Table.AddColumn(#"Added DayOfWeekNumber", "DayOfWeekName", each Date.DayOfWeekName(DateTime.Date([Date])), type text),
    #"Added DayOfWeekNameSort" = Table.AddColumn(#"Added DayOfWeekName", "DayOfWeekNameSort", each Date.DayOfWeek(DateTime.Date([Date]), Day.Monday) + 1, Int64.Type),
    #"Added DayOfWeekNameShort" = Table.AddColumn(#"Added DayOfWeekNameSort", "DayOfWeekNameShort", each Text.Start(Date.DayOfWeekName(DateTime.Date([Date])), 3), type text),
    #"Added DayOfWeekNameShortSort" = Table.AddColumn(#"Added DayOfWeekNameShort", "DayOfWeekNameShortSort", each Date.DayOfWeek(DateTime.Date([Date]), Day.Monday) + 1, Int64.Type),
    #"Added DayOfWeekAbbr" = Table.AddColumn(#"Added DayOfWeekNameShortSort", "DayOfWeekAbbr", each Text.Start(Date.DayOfWeekName(DateTime.Date([Date])), 2), type text),
    #"Added DayOfWeekAbbrSort" = Table.AddColumn(#"Added DayOfWeekAbbr", "DayOfWeekAbbrSort", each Date.DayOfWeek(DateTime.Date([Date]), Day.Monday) + 1, Int64.Type),
    #"Added DayOfYear" = Table.AddColumn(#"Added DayOfWeekAbbrSort", "DayOfYear", each Date.DayOfYear(DateTime.Date([Date])), Int64.Type),
    
    // Fiscal Year columns
    #"Added FiscalYear" = Table.AddColumn(#"Added DayOfYear", "FiscalYear", 
        each if Date.Month(DateTime.Date([Date])) >= FiscalYearStartMonth 
             then Date.Year(DateTime.Date([Date])) + 1 
             else Date.Year(DateTime.Date([Date])), Int64.Type),
    #"Added FiscalQuarter" = Table.AddColumn(#"Added FiscalYear", "FiscalQuarter", 
        each Number.RoundUp((Date.Month(DateTime.Date([Date])) - FiscalYearStartMonth + 13) / 3) - 
             (if Date.Month(DateTime.Date([Date])) >= FiscalYearStartMonth then 0 else 4), Int64.Type),
    #"Added FiscalQuarterName" = Table.AddColumn(#"Added FiscalQuarter", "FiscalQuarterName", 
        each "FY" & Text.End(Text.From([FiscalYear]), 2) & " Q" & Text.From([FiscalQuarter]), type text),
    #"Added FiscalQuarterNameSort" = Table.AddColumn(#"Added FiscalQuarterName", "FiscalQuarterNameSort", 
        each [FiscalYear] * 10 + [FiscalQuarter], Int64.Type),
    #"Added FiscalMonth" = Table.AddColumn(#"Added FiscalQuarterNameSort", "FiscalMonth", 
        each if Date.Month(DateTime.Date([Date])) >= FiscalYearStartMonth 
             then Date.Month(DateTime.Date([Date])) - FiscalYearStartMonth + 1 
             else Date.Month(DateTime.Date([Date])) + 13 - FiscalYearStartMonth, Int64.Type),
    
    // Flags and indicators
    #"Added IsWeekend" = Table.AddColumn(#"Added FiscalMonth", "IsWeekend", 
        each if Date.DayOfWeek(DateTime.Date([Date]), Day.Monday) >= 5 then true else false, type logical),
    #"Added IsWeekday" = Table.AddColumn(#"Added IsWeekend", "IsWeekday", 
        each not [IsWeekend], type logical),
    #"Added IsCurrentMonth" = Table.AddColumn(#"Added IsWeekday", "IsCurrentMonth", 
        each Date.Month(DateTime.Date([Date])) = Date.Month(DateTime.LocalNow()) and Date.Year(DateTime.Date([Date])) = Date.Year(DateTime.LocalNow()), type logical),
    #"Added IsCurrentQuarter" = Table.AddColumn(#"Added IsCurrentMonth", "IsCurrentQuarter", 
        each Date.QuarterOfYear(DateTime.Date([Date])) = Date.QuarterOfYear(DateTime.LocalNow()) and Date.Year(DateTime.Date([Date])) = Date.Year(DateTime.LocalNow()), type logical),
    #"Added IsCurrentYear" = Table.AddColumn(#"Added IsCurrentQuarter", "IsCurrentYear", 
        each Date.Year(DateTime.Date([Date])) = Date.Year(DateTime.LocalNow()), type logical),
    
    // Relative periods
    #"Added MonthOffset" = Table.AddColumn(#"Added IsCurrentYear", "MonthOffset", 
        each (Date.Year(DateTime.Date([Date])) - Date.Year(DateTime.LocalNow())) * 12 + (Date.Month(DateTime.Date([Date])) - Date.Month(DateTime.LocalNow())), Int64.Type),
    #"Added QuarterOffset" = Table.AddColumn(#"Added MonthOffset", "QuarterOffset", 
        each (Date.Year(DateTime.Date([Date])) - Date.Year(DateTime.LocalNow())) * 4 + (Date.QuarterOfYear(DateTime.Date([Date])) - Date.QuarterOfYear(DateTime.LocalNow())), Int64.Type),
    #"Added YearOffset" = Table.AddColumn(#"Added QuarterOffset", "YearOffset", 
        each Date.Year(DateTime.Date([Date])) - Date.Year(DateTime.LocalNow()), Int64.Type)
in
    #"Added YearOffset";
shared Query_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "6f688f9a-787a-46c6-8068-8d6e88edd051"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "cfa259ab-5ffa-439c-baa6-8b58678a13de"]}[Data],
  TableNavigation = Navigation_2{[Item = "Dim_Date", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
