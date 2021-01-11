package distributed.system.distributed.document.search.search;

import distributed.system.distributed.document.search.model.DocumentData;

import java.util.*;

public class TFIDF {

    public static double calculateTermFrequency(List<String> documentTerms, String queryTerm) {
        long count = 0;
        for(String documentTerm: documentTerms) {
            if(queryTerm.equalsIgnoreCase(documentTerm)) {
                count++;
            }
        }
        return (double)count/ documentTerms.size();
    }

    public static DocumentData createDocumentData(List<String> documentTerms, List<String> queryTerms) {
        DocumentData documentData = new DocumentData();
        for(String queryTerm: queryTerms) {
            double termFreq = calculateTermFrequency(documentTerms, queryTerm);
            documentData.putTermFrequency(queryTerm, termFreq);
        }
        return documentData;
    }

    private static double getInverseDocumentFrequency(String term, Map<String, DocumentData> documentResults) {
        double n = 0;
        for(String document: documentResults.keySet()) {
            DocumentData documentData = documentResults.get(document);
            double termFrequency = documentData.getFrequency(term);
            if(termFrequency > 0.0) {
                n++;
            }
        }
        return n == 0 ? 0 : Math.log10(documentResults.size()/n);
    }

    private static Map<String, Double> getTermToInverseDocumentFrequencyMap(List<String> queryTerms, Map<String,DocumentData> documentResults) {
        Map<String, Double> termToIDF = new HashMap<>();
        for(String queryTerm : queryTerms) {
            double idf = getInverseDocumentFrequency(queryTerm, documentResults);
            termToIDF.put(queryTerm,idf);
        }
        return termToIDF;
    }

    private static double calculateDocumentScore(List<String> queryTerms, DocumentData documentData, Map<String,Double> termToInverseDocumentFrequency) {
        double score = 0;
        for(String queryTerm : queryTerms) {
            double termFrequency = documentData.getFrequency(queryTerm);
            double inverseTermFrequency = termToInverseDocumentFrequency.get(queryTerm);
            score += termFrequency*inverseTermFrequency;
        }
        return score;
    }

    public static Map<Double, List<String>> getDocumentsSortedByScore(List<String> queryTerms, Map<String, DocumentData> documentResults) {
        TreeMap<Double, List<String>> scoreToDocuments = new TreeMap<>();
        Map<String, Double> termToInverseDocumentFrequency = getTermToInverseDocumentFrequencyMap(queryTerms,documentResults);
        for(String document : documentResults.keySet()) {
            DocumentData documentData = documentResults.get(document);
            double score = calculateDocumentScore(queryTerms, documentData, termToInverseDocumentFrequency);
            addDocumentScoreToTreeMap(scoreToDocuments, score, document);
        }
        return scoreToDocuments.descendingMap();
    }

    private static void addDocumentScoreToTreeMap(TreeMap<Double, List<String>> scoreToDocuments, double score, String document) {
        List<String> documentWithCurrentScore = scoreToDocuments.get(score);
        if(documentWithCurrentScore == null) {
            documentWithCurrentScore = new ArrayList<>();
        }
        documentWithCurrentScore.add(document);
        scoreToDocuments.put(score, documentWithCurrentScore);
    }

    public static List<String> getWordsFromLine(String line) {
        return Arrays.asList(line.split("(\\.)+|(,)+|( )+|(-)+|(\\?)+|(!)+|(;)+|(:)+|(/d)+|(/n)+"));
    }

    public static List<String> getWordsFromLines(List<String> lines) {
        List<String> words = new ArrayList<>();
        for(String line: lines) {
            words.addAll(getWordsFromLine(line));
        }
        return words;
    }
}
