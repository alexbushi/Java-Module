import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DataCruncherTest {
    private final DataCruncher dataCruncher = new DataCruncher();

    // ignore
    @Test
    public void readAllTransactions() throws Exception {
        var transactions = dataCruncher.readAllTransactions();
        assertEquals(594643, transactions.size());
    }

    // example
    @Test
    public void readAllTransactionsAge0() throws Exception {
        var transactions = dataCruncher.readAllTransactionsAge0();
        assertEquals(3630, transactions.size());
    }

    // task1
    @Test
    public void getUniqueMerchantIds() throws Exception {
        var transactions = dataCruncher.getUniqueMerchantIds();
        assertEquals(50, transactions.size());
    }

    // task2
    @Test
    public void getTotalNumberOfFraudulentTransactions() throws Exception {
        var totalNumberOfFraudulentTransactions = dataCruncher.getTotalNumberOfFraudulentTransactions();
        assertEquals(297508, totalNumberOfFraudulentTransactions);
    }

    // task3
    @Test
    public void getTotalNumberOfTransactions() throws Exception {
        assertEquals(297508, dataCruncher.getTotalNumberOfTransactions(true));
        assertEquals(297135, dataCruncher.getTotalNumberOfTransactions(false));
    }

    // task4
    @Test
    public void getFraudulentTransactionsForMerchantId() throws Exception {
        Set<Transaction> fraudulentTransactionsForMerchantId = dataCruncher
                .getFraudulentTransactionsForMerchantId("M1823072687");
        assertEquals(149001, fraudulentTransactionsForMerchantId.size());
    }

    // task5
    @Test
    public void getTransactionForMerchantId() throws Exception {
        assertEquals(102588, dataCruncher.getTransactionsForMerchantId("M348934600", true).size());
        assertEquals(102140, dataCruncher.getTransactionsForMerchantId("M348934600", false).size());
    }

    // task6
    @Test
    public void getAllTransactionSortedByAmount() throws Exception {
        List<Transaction> allTransactionsSortedByAmount = dataCruncher.getAllTransactionsSortedByAmount();
        double previousTransactionAmount = 0.0;
        for (int i = 0; i < allTransactionsSortedByAmount.size(); i++) {
            if (allTransactionsSortedByAmount.get(i).getAmount() >= previousTransactionAmount) {
                continue;
            } else {
                fail();
            }
        }
        assertEquals(1, 1);
    }

    // task7
    @Test
    public void getFraudPercentageForMen() throws Exception {
        double fraudPercentageForMen = dataCruncher.getFraudPercentageForMen();
        assertEquals(0.45, fraudPercentageForMen, 0.01);
    }

    @Test
    public void getCustomerIdsWithNumberOfFraudulentTransactions() throws Exception {
        Map<String, String> taskTestMap = new LinkedHashMap<String, String>();
        taskTestMap.put("test1", "[]");
        taskTestMap.put("test2", "[]");
        taskTestMap.put("test3", "[C]");
        taskTestMap.put("test4", "[C]");
        taskTestMap.put("test5", "[]");
        taskTestMap.put("test6", "[C]");
        taskTestMap.put("test7", "[C, D]");
        taskTestMap.put("test8", "[]");

        Integer testThreshold = 2;

        taskTestMap.forEach((csvName, expectedValue) -> {
            System.out.println(csvName + " Got | Expected");
            try {
                System.out.println(dataCruncher.getCustomerIdsWithNumberOfFraudulentTransactions(testThreshold, csvName)
                        .toString());
                System.out.println(expectedValue + "\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                assertEquals(expectedValue, dataCruncher
                        .getCustomerIdsWithNumberOfFraudulentTransactions(testThreshold, csvName).toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    // task9
    @Test
    public void getCustomerIdToNumberOfTransactions() throws Exception {
        // Tests under testPaymentsx.csv
        // 1 Test not fraud
        // 2 Test fraud
        // 3 Test two fraud
        // 4 Test two not fraud
        // 5 Test 1 fraud, 1 not fraud
        // 6 Test two users, both no fraud
        // 7 Test two users, both fraud
        // 8 Test two usres, 1 fraud, 1 no fraud
        // 9 Test two users, 2 fraud, 1 fraud
        // 10 Test blank customer id

        Map<String, String> task9TestMap = new LinkedHashMap<String, String>();
        task9TestMap.put("testPayments1", "{C1093826151=0}");
        task9TestMap.put("testPayments2", "{C1093826151=1}");
        task9TestMap.put("testPayments3", "{C1093826151=2}");
        task9TestMap.put("testPayments4", "{C1093826151=0}");
        task9TestMap.put("testPayments5", "{C1093826151=1}");
        task9TestMap.put("testPayments6", "{D1093826151=0, C1093826151=0}");
        task9TestMap.put("testPayments7", "{D1093826151=1, C1093826151=1}");
        task9TestMap.put("testPayments8", "{D1093826151=1, C1093826151=0}");
        task9TestMap.put("testPayments9", "{D1093826151=1, C1093826151=0}");
        task9TestMap.put("testPayments10", "{=0, D1093826151=1, C1093826151=0}");

        task9TestMap.forEach((csvName, expectedValue) -> {
            System.out.println(csvName + " Got | Expected");
            try {
                System.out.println(dataCruncher.getCustomerIdToNumberOfTransactions(csvName).toString());
                System.out.println(expectedValue + "\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                assertEquals(expectedValue, dataCruncher.getCustomerIdToNumberOfTransactions(csvName).toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    // task10
    @Test
    public void getMerchantIdToTotalAmountOfFraudulentTransactions() throws Exception {
        Map<String, String> taskTestMap = new LinkedHashMap<String, String>();
        taskTestMap.put("t1", "{M=0.0}");
        taskTestMap.put("t2", "{M=1.0}");
        taskTestMap.put("t3", "{M=3.0}");
        taskTestMap.put("t4", "{E=0.0, M=4.0}");
        taskTestMap.put("t5", "{E=2.0, M=4.0}");
        taskTestMap.put("t6", "{E=0.0, M=0.0}");

        taskTestMap.forEach((csvName, expectedValue) -> {
            System.out.println(csvName + " Got | Expected");
            try {
                System.out.println(dataCruncher.getMerchantIdToTotalAmountOfFraudulentTransactions(csvName).toString());
                System.out.println(expectedValue + "\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                assertEquals(expectedValue,
                        dataCruncher.getMerchantIdToTotalAmountOfFraudulentTransactions(csvName).toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}