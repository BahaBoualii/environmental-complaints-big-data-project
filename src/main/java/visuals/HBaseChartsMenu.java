package visuals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.jfree.chart.*;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class HBaseChartsMenu {

    private static Connection connection;
    private static Table table;

    public static void main(String[] args) {
        try {
            Configuration config = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("cdph_complaints"));

            Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.println("\n=== Environmental Complaints Visualization ===");
                System.out.println("1. Bar Chart – Top Complaint Types");
                System.out.println("2. Line Chart – Complaints Per Year");
                System.out.println("3. Pie Chart – Resolved vs Unresolved");
                System.out.println("4. Area Chart – Complaint Trends Over Time");
                System.out.println("0. Exit");
                System.out.print("Select an option: ");
                int choice = scanner.nextInt();

                switch (choice) {
                    case 1:
                        barChartComplaintTypes();
                        break;
                    case 2:
                        lineChartComplaintsPerYear();
                        break;
                    case 3:
                        pieChartResolutionStatus();
                        break;
                    case 4:
                        areaChartComplaintTrends();
                        break;
                    case 0:
                        System.out.println("Exiting.");
                        table.close();
                        connection.close();
                        return;
                    default:
                        System.out.println("Invalid option. Please try again.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void barChartComplaintTypes() throws IOException {
        Map<String, Integer> typeCounts = new HashMap<>();
        Scan scan = new Scan().setRowPrefixFilter(Bytes.toBytes("type#"));
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            String type = Bytes.toString(result.getValue(Bytes.toBytes("details"), Bytes.toBytes("type")));
            try {
                int count = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("details"), Bytes.toBytes("value"))));
                typeCounts.put(type, count);
            } catch (NumberFormatException e) {
                System.out.println("Warning: Could not parse count for type " + type + ": " +
                        Bytes.toString(result.getValue(Bytes.toBytes("details"), Bytes.toBytes("value"))));
                typeCounts.put(type, 0);
            }
        }
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        typeCounts.forEach((k, v) -> dataset.addValue(v, "Complaints", k));
        JFreeChart chart = ChartFactory.createBarChart("Top Complaint Types", "Type", "Count", dataset, PlotOrientation.VERTICAL, false, true, false);
        ChartUtils.saveChartAsPNG(new File("TopComplaintTypes.png"), chart, 800, 600);
        System.out.println("Saved: TopComplaintTypes.png");
    }

    private static void lineChartComplaintsPerYear() throws IOException {
        TreeMap<Integer, Integer> yearCounts = new TreeMap<>();

        Scan scan = new Scan().setRowPrefixFilter(Bytes.toBytes("year#"));
        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {
            String rowKey = Bytes.toString(result.getRow());  // should be in format "year#YYYY"

            if (rowKey == null || !rowKey.contains("#")) continue;

            String[] parts = rowKey.split("#", 2);
            if (parts.length < 2) continue;

            int year;
            try {
                year = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid year key: " + rowKey);
                continue;
            }

            String countStr = Bytes.toString(result.getValue(Bytes.toBytes("details"), Bytes.toBytes("value")));
            int count;
            try {
                count = Integer.parseInt(countStr);
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid count: " + countStr);
                continue;
            }

            yearCounts.put(year, count);
        }

        XYSeries series = new XYSeries("Complaints");
        for (Map.Entry<Integer, Integer> entry : yearCounts.entrySet()) {
            series.add(entry.getKey(), entry.getValue());
        }

        XYSeriesCollection dataset = new XYSeriesCollection(series);
        JFreeChart chart = ChartFactory.createXYLineChart(
                "Complaints Per Year",
                "Year", "Count",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);

        ChartUtils.saveChartAsPNG(new File("ComplaintsPerYear.png"), chart, 800, 600);
        System.out.println("Saved: ComplaintsPerYear.png");
    }

    private static void pieChartResolutionStatus() throws IOException {
        Map<String, Integer> statusCounts = new HashMap<>();
        Scan scan = new Scan().setRowPrefixFilter(Bytes.toBytes("status#"));
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            String status = Bytes.toString(result.getValue(Bytes.toBytes("status"), Bytes.toBytes("status_type")));
            try {
                int count = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("status"), Bytes.toBytes("value"))));
                statusCounts.put(status, count);
            } catch (NumberFormatException e) {
                System.out.println("Warning: Could not parse count for status " + status + ": " +
                        Bytes.toString(result.getValue(Bytes.toBytes("status"), Bytes.toBytes("value"))));
                statusCounts.put(status, 0);
            }
        }
        DefaultPieDataset dataset = new DefaultPieDataset();
        statusCounts.forEach(dataset::setValue);
        JFreeChart chart = ChartFactory.createPieChart("Resolved vs Unresolved", dataset, true, true, false);
        ChartUtils.saveChartAsPNG(new File("ResolvedVsUnresolved.png"), chart, 600, 600);
        System.out.println("Saved: ResolvedVsUnresolved.png");
    }

    private static void areaChartComplaintTrends() throws IOException {
        Map<String, TreeMap<Integer, Integer>> typeYearMap = new HashMap<>();

        Scan scan = new Scan().setRowPrefixFilter(Bytes.toBytes("trend#"));
        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {
            String key = Bytes.toString(result.getValue(Bytes.toBytes("details"), Bytes.toBytes("type_year")));

            if (key == null || !key.contains("#")) continue;

            String[] parts = key.split("#", 2);
            if (parts.length < 2) continue;

            int year;
            try {
                year = Integer.parseInt(parts[0]);
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid year: " + parts[0]);
                continue;
            }

            String type = parts[1];

            String countStr = Bytes.toString(result.getValue(Bytes.toBytes("details"), Bytes.toBytes("value")));
            int count;
            try {
                count = Integer.parseInt(countStr);
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid count: " + countStr);
                continue;
            }

            typeYearMap.computeIfAbsent(type, k -> new TreeMap<>()).put(year, count);
        }

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (String type : typeYearMap.keySet()) {
            for (Map.Entry<Integer, Integer> entry : typeYearMap.get(type).entrySet()) {
                dataset.addValue(entry.getValue(), type, entry.getKey());
            }
        }

        JFreeChart chart = ChartFactory.createStackedAreaChart(
                "Complaint Type Trends Over Time",
                "Year", "Count",
                dataset, PlotOrientation.VERTICAL,
                true, true, false);

        ChartUtils.saveChartAsPNG(new File("ComplaintTrends.png"), chart, 1000, 600);
        System.out.println("Saved: ComplaintTrends.png");
    }

}