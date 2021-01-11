package distributed.systems.document;

import distributed.systems.document.model.DocumentData;
import distributed.systems.document.search.TFIDF;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SequentialSearch {
    public static final String BOOK_DIRECTORY = "books";
    public static final String SEARCH_QUERY_1 = "the best detective that catches any criminal using his deductive methods";
    public static final String SEARCH_QUERY_2 = "the girl that falls through a rabbit hole inta a fantasy wonderland";
    public static final String SEARCH_QUERY_3 = "A war between russia through and france in the cold winter";

    public static void main(String[] args) throws FileNotFoundException, URISyntaxException {
        ClassLoader classLoader = SequentialSearch.class.getClassLoader();
        URL resource = classLoader.getResource(BOOK_DIRECTORY);
        File documentDirectory = new File(resource.toURI());

        List<String> documents = Arrays.stream(documentDirectory.list())
                .map(documentName -> BOOK_DIRECTORY + "/" + documentName)
                .collect(Collectors.toList());

        List<String> queryTerms = TFIDF.getWordsFromLine(SEARCH_QUERY_3);

        findMostRelevantDocuments(documents, queryTerms);
    }

    private static void findMostRelevantDocuments(List<String> documents, List<String> queryTerms) throws FileNotFoundException, URISyntaxException {
        Map<String, DocumentData> documentDataMap = new HashMap<>();

        ClassLoader classLoader = SequentialSearch.class.getClassLoader();


        for(String document : documents) {
            URL resource = classLoader.getResource(document);
            File documentDirectory = new File(resource.toURI());
            BufferedReader bufferedReader = new BufferedReader(new FileReader(documentDirectory));
            List<String> lines = bufferedReader.lines().collect(Collectors.toList());
            List<String> documentTerms = TFIDF.getWordsFromLines(lines);
            DocumentData documentData = TFIDF.createDocumentData(documentTerms,queryTerms);
            documentDataMap.put(document,documentData);
        }

        Map<Double, List<String>> documentByScore = TFIDF.getDocumentsSortedByScore(queryTerms, documentDataMap);
        printResults(documentByScore);
    }

    private static void printResults(Map<Double, List<String>> documentByScore) {
        for(Map.Entry<Double, List<String>> documentScorePair : documentByScore.entrySet()) {
            double score = documentScorePair.getKey();
            for(String document : documentScorePair.getValue()) {
                System.out.println(String.format("Book : %s - score  %f", document.split("/")[1],score));
            }
        }
    }

}
