import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataCruncher {

    // do not modify this method - just use it to get all the Transactions that are
    // in scope for the exercise
    public List<Transaction> readAllTransactions() throws Exception {
        return Files.readAllLines(Paths.get("src/main/resources/payments.csv"), StandardCharsets.UTF_8).stream().skip(1)
                .map(line -> {
                    var commaSeparatedLine = List.of(line.replaceAll("'", "").split(","));
                    var ageString = commaSeparatedLine.get(2);
                    var ageInt = "U".equals(ageString) ? 0 : Integer.parseInt(ageString);
                    return new Transaction(commaSeparatedLine.get(1), ageInt, commaSeparatedLine.get(3),
                            commaSeparatedLine.get(4), commaSeparatedLine.get(5), commaSeparatedLine.get(6),
                            commaSeparatedLine.get(7), Double.parseDouble(commaSeparatedLine.get(8)),
                            "1".equals(commaSeparatedLine.get(9)));
                }).collect(Collectors.toList());
    }

    public List<Transaction> readAllTransactionsCustomCsv(String csvName) throws Exception {
        try {
            return Files.readAllLines(Paths.get("src/main/resources/" + csvName + ".csv"), StandardCharsets.UTF_8)
                    .stream().skip(1).map(line -> {
                        var commaSeparatedLine = List.of(line.replaceAll("'", "").split(","));
                        var ageString = commaSeparatedLine.get(2);
                        var ageInt = "U".equals(ageString) ? 0 : Integer.parseInt(ageString);
                        return new Transaction(commaSeparatedLine.get(1), ageInt, commaSeparatedLine.get(3),
                                commaSeparatedLine.get(4), commaSeparatedLine.get(5), commaSeparatedLine.get(6),
                                commaSeparatedLine.get(7), Double.parseDouble(commaSeparatedLine.get(8)),
                                "1".equals(commaSeparatedLine.get(9)));
                    }).collect(Collectors.toList());
        } catch (Exception e) {
            return readAllTransactions();
        }

    }

    // example
    public List<Transaction> readAllTransactionsAge0() throws Exception {
        return readAllTransactions().stream().filter(transaction -> transaction.getAge() == 0)
                .collect(Collectors.toList());
    }

    // public void readAllTransactionsIDCheck() throws Exception {
    // System.out.println(readAllTransactions().stream().filter(transaction ->
    // transaction.getMerchantId().equals(""))
    // .collect(Collectors.toList()));
    // }

    // task 1
    public Set<String> getUniqueMerchantIds() throws Exception {
        Set<String> setOfUniqueMerchantIds = new HashSet<String>();
        Stream<Transaction> distinctMerchantIdElements = readAllTransactions().stream().distinct();
        distinctMerchantIdElements.forEach(transaction -> setOfUniqueMerchantIds.add(transaction.getMerchantId()));
        return setOfUniqueMerchantIds;
    }

    // task 2
    public long getTotalNumberOfFraudulentTransactions() throws Exception {
        return readAllTransactions().stream().filter(transaction -> transaction.isFraud() == true)
                .collect(Collectors.toList()).size();
    }

    // task 3
    public long getTotalNumberOfTransactions(boolean isFraud) throws Exception {
        return readAllTransactions().stream().filter(transaction -> transaction.isFraud() == isFraud)
                .collect(Collectors.toList()).size();
    }

    // task 4
    public Set<Transaction> getFraudulentTransactionsForMerchantId(String merchantId) throws Exception {
        return readAllTransactions().stream()
                .filter(transaction -> transaction.getMerchantId().equals(merchantId) && transaction.isFraud() == true)
                .collect(Collectors.toSet());
    }

    // task 5
    public Set<Transaction> getTransactionsForMerchantId(String merchantId, boolean isFraud) throws Exception {
        return readAllTransactions().stream().filter(
                transaction -> transaction.getMerchantId().equals(merchantId) && transaction.isFraud() == isFraud)
                .collect(Collectors.toSet());
    }

    // task 6
    public List<Transaction> getAllTransactionsSortedByAmount() throws Exception {
        return readAllTransactions().stream().sorted((o1, o2) -> o1.getAmount().compareTo(o2.getAmount()))
                .collect(Collectors.toList());

    }

    // task 7
    public double getFraudPercentageForMen() throws Exception {
        long totalAmountOfFraudTransactions = getTotalNumberOfFraudulentTransactions();
        long totalAmountOfFraudTransactionsMen = readAllTransactions().stream()
                .filter(transaction -> transaction.isFraud() == true && transaction.getGender().equals("M"))
                .collect(Collectors.toList()).size();
        // check / by 0
        double percent = (double) totalAmountOfFraudTransactionsMen / totalAmountOfFraudTransactions;
        return percent;
    }

    // task 8
    public Set<String> getCustomerIdsWithNumberOfFraudulentTransactions(int numberOfFraudulentTransactions,
            String csvName) throws Exception {
        Set<String> setOfCustomerIds = new HashSet<String>();
        getCustomerIdToNumberOfTransactions(csvName).forEach((customerId, numOfFraudTransactions) -> {
            if (numOfFraudTransactions >= numberOfFraudulentTransactions) {
                setOfCustomerIds.add(customerId);
            }
        });

        return setOfCustomerIds;
    }

    // task 9
    public Map<String, Integer> getCustomerIdToNumberOfTransactions(String csvName) throws Exception {
        HashMap<String, Integer> mapOfIdAndNumOfFraudTransactions = new HashMap<String, Integer>();

        readAllTransactionsCustomCsv(csvName).stream().forEach(transaction -> {
            // For each fraudulent transaction get the customer id
            String currentCustomerId = transaction.getCustomerId();
            // Check to see if customer is already in map, Can be a value or null
            Integer previousValueAtKey = mapOfIdAndNumOfFraudTransactions.put(currentCustomerId, 0);

            if (previousValueAtKey == null) {
                // New customer id not yet in map
                if (transaction.isFraud()) {
                    mapOfIdAndNumOfFraudTransactions.put(currentCustomerId, 1);
                } else {
                    mapOfIdAndNumOfFraudTransactions.put(currentCustomerId, 0);
                }
            } else {
                // Increment number of fraudulent transactions for this customer
                if (transaction.isFraud()) {
                    mapOfIdAndNumOfFraudTransactions.put(currentCustomerId, previousValueAtKey + 1);
                } else {
                    mapOfIdAndNumOfFraudTransactions.put(currentCustomerId, previousValueAtKey);
                }
            }
        });
        return mapOfIdAndNumOfFraudTransactions;
    }

    // task 10
    public Map<String, Double> getMerchantIdToTotalAmountOfFraudulentTransactions(String csvName) throws Exception {
        HashMap<String, Double> mapOfIdAndFraudTransactions = new HashMap<String, Double>();

        readAllTransactionsCustomCsv(csvName).stream().forEach(transactionForThisMerchant -> {
            Double currentFraudTransactionAmt = transactionForThisMerchant.getAmount();
            String merchantIdString = transactionForThisMerchant.getMerchantId();
            // If a new key is passed, put() returns null
            // If existing key is passed, previous value is returned
            Double previousMerchantFraudAmount = mapOfIdAndFraudTransactions.put(merchantIdString, 0.0);

            // existing merchant (implies existing key)
            if (previousMerchantFraudAmount != null) {
                // add up fraud amount
                if (transactionForThisMerchant.isFraud()) {
                    mapOfIdAndFraudTransactions.put(merchantIdString,
                            previousMerchantFraudAmount + currentFraudTransactionAmt);
                } else {
                    // no fraud so replace with previous
                    mapOfIdAndFraudTransactions.put(merchantIdString, previousMerchantFraudAmount);
                }
                // new merchant (implies new key)
            } else {
                if (transactionForThisMerchant.isFraud()) {
                    mapOfIdAndFraudTransactions.put(merchantIdString, currentFraudTransactionAmt);
                } else {
                    mapOfIdAndFraudTransactions.put(merchantIdString, 0.0);
                }
            }
        }

        );

        return mapOfIdAndFraudTransactions;
    }
}
