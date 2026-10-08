# RFQ Report — switch the chart grouping

## Goal

Let users switch one chart between **RFQs by Status Reason** and **RFQs by Customer Status**. Both views use the existing measure `[No.of RFQs]`.

## Create a field parameter in Power BI Desktop

1. Open **Modeling → New parameter → Fields**.
2. Name the parameter **RFQ View**.
3. Add your existing **Status Reason** and **Customer Status** columns, in that order. They may belong to different tables.
4. Rename the display labels to **RFQs by Status Reason** and **RFQs by Customer Status**.
5. Check **Add slicer to this page**, then select **Create**.

Use the Fields parameter dialog rather than creating an ordinary calculated table: Power BI adds the field parameter metadata needed to switch the visual's fields.

Power BI generates a definition like this:

```dax
RFQ View = {
    ("RFQs by Status Reason", NAMEOF('Your RFQ Table'[Status Reason]), 0),
    ("RFQs by Customer Status", NAMEOF('Your RFQ Table'[Customer Status]), 1)
}
```

**`Your RFQ Table` is a placeholder.** Select the actual columns in the dialog, or replace each table reference with the correct table in your model. Do not change the existing `[No.of RFQs]` measure.

## Update the existing chart

1. Remove **Status Reason** from the chart's category axis.
2. Add the **RFQ View** field parameter to that category axis.
3. Keep **No.of RFQs** on the value axis.
4. Select the generated **RFQ View** slicer. Turn **Single select** on and **Select all** off.
5. Use a tile/button slicer layout to display the two options as a toggle.
6. Select **RFQs by Status Reason** as the initial view and save the report.
7. Confirm the slicer filters this chart under **Format → Edit interactions**.

### Axis orientation

- **Vertical column chart:** X-axis = RFQ View; Y-axis = No.of RFQs.
- **Horizontal bar chart:** Y-axis = RFQ View; X-axis = No.of RFQs.

The parameter belongs on the category axis, whichever orientation your chart uses.

## Optional dynamic title

Create this measure if you want the chart title to follow the selected view:

```dax
RFQ Chart Title =
VAR SelectedViews =
    SELECTCOLUMNS(
        SUMMARIZE(
            'RFQ View',
            'RFQ View'[RFQ View],
            'RFQ View'[RFQ View Fields]
        ),
        "ViewLabel", 'RFQ View'[RFQ View]
    )
RETURN
    IF(
        COUNTROWS(SelectedViews) = 1,
        CONCATENATEX(SelectedViews, [ViewLabel], ""),
        "RFQs by selected view"
    )
```

Set **Format visual → General → Title → Text → fx → Format style: Field value** to **RFQ Chart Title**.

The title measure includes the parameter's Fields column because the display label is part of a composite key; a direct SELECTEDVALUE on the label can fail.

## Validate in the report

- Select **RFQs by Status Reason**: the categories should be status reasons.
- Select **RFQs by Customer Status**: the categories should be customer statuses.
- Confirm only one option can be selected and the measure remains **No.of RFQs**.
- Compare each view with a temporary table containing its grouping column and **No.of RFQs**, using the same report filters.
- If Customer Status does not filter RFQs correctly, check the relationship path from its table to the RFQ data and whether the measure removes that filter.
- Check any existing visual filters on Status Reason: they can continue restricting RFQs in the Customer Status view.
- Save, reopen, and confirm the default selection.

## Implementation status

This folder contains the setup instructions and DAX template. No PBIX/PBIP report or model schema was supplied, so the actual report has not been modified or validated in Power BI.
