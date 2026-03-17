import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Tema1 {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Pattern SPLIT_PATTERN = Pattern.compile("[\\s]+");

    public static Set<String> targetLanguages = new HashSet<>();
    public static Set<String> targetCategories = new HashSet<>();
    public static Set<String> stopWords = new HashSet<>();
    public static List<String> inputFiles = new ArrayList<>();

    public static final List<List<Article>> collectedResults = Collections.synchronizedList(new ArrayList<>());

    public static ConcurrentHashMap<String, Integer> titleCounts = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, Integer> uuidCounts = new ConcurrentHashMap<>();

    public static ConcurrentHashMap<String, AtomicInteger> keywordCounts = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, AtomicInteger> authorCounts = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, AtomicInteger> langCounts = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, AtomicInteger> catCounts = new ConcurrentHashMap<>();

    public static AtomicInteger duplicatesFound = new AtomicInteger(0);
    public static AtomicInteger uniqueArticlesCount = new AtomicInteger(0);
    public static AtomicInteger fileIndex = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
       if (args.length < 3) return;

       int numThreads = Integer.parseInt(args[0]);
       String articlesFile = args[1];
       String auxFile = args[2];

       readAuxiliaryFiles(auxFile);
       readInputFilesList(articlesFile);

       CyclicBarrier barrier = new CyclicBarrier(numThreads);
       Thread[] threads = new Thread[numThreads];

       for (int i = 0; i < numThreads; i++) {
          threads[i] = new Thread(new Worker(i, numThreads, barrier));
          threads[i].start();
       }

       for (Thread t : threads) {
          t.join();
       }
    }

    static class Worker implements Runnable {
       int id;
       int totalThreads;
       CyclicBarrier barrier;

       List<Article> myAllArticles = new ArrayList<>();
       List<Article> myValidArticles = new ArrayList<>();

       public Worker(int id, int totalThreads, CyclicBarrier barrier) {
          this.id = id;
          this.totalThreads = totalThreads;
          this.barrier = barrier;
       }

       @Override
       public void run() {
          try {
             Map<String, Integer> localTitleCounts = new HashMap<>();
             Map<String, Integer> localUuidCounts = new HashMap<>();

             while (true) {
                int idx = fileIndex.getAndIncrement();
                if (idx >= inputFiles.size()) break;

                File file = new File(inputFiles.get(idx));
                try {
                   Article[] articles = mapper.readValue(file, Article[].class);
                   for (Article a : articles) {
                      myAllArticles.add(a);

                      Integer countT = localTitleCounts.get(a.title);
                      if (countT == null) {
                          localTitleCounts.put(a.title, 1);
                      } else {
                          localTitleCounts.put(a.title, countT + 1);
                      }

                      Integer countU = localUuidCounts.get(a.uuid);
                      if (countU == null) {
                          localUuidCounts.put(a.uuid, 1);
                      } else {
                          localUuidCounts.put(a.uuid, countU + 1);
                      }
                   }
                } catch (Exception e) {
                }
             }

             for (Map.Entry<String, Integer> entry : localTitleCounts.entrySet()) {
                 titleCounts.merge(entry.getKey(), entry.getValue(), new java.util.function.BiFunction<Integer, Integer, Integer>() {
                     @Override
                     public Integer apply(Integer a, Integer b) {
                         return a + b;
                     }
                 });
             }

             for (Map.Entry<String, Integer> entry : localUuidCounts.entrySet()) {
                 uuidCounts.merge(entry.getKey(), entry.getValue(), new java.util.function.BiFunction<Integer, Integer, Integer>() {
                     @Override
                     public Integer apply(Integer a, Integer b) {
                         return a + b;
                     }
                 });
             }

             barrier.await();
             Map<String, Integer> localKeywords = new HashMap<>();
             Map<String, Integer> localAuthors = new HashMap<>();
             Map<String, Integer> localLangs = new HashMap<>();
             Map<String, Integer> localCats = new HashMap<>();

             for (Article a : myAllArticles) {
                Integer tCount = titleCounts.get(a.title);
                Integer uCount = uuidCounts.get(a.uuid);

                if ((tCount != null && tCount > 1) || (uCount != null && uCount > 1)) {
                   duplicatesFound.incrementAndGet();
                   continue;
                }

                myValidArticles.add(a);

                String auth = a.author;
                if (auth == null) auth = "Unknown";
                addToLocalMap(localAuthors, auth);

                if (targetLanguages.contains(a.language)) {
                   addToLocalMap(localLangs, a.language);
                }

                if (a.categories != null) {
                   Set<String> distinctCats = new HashSet<>();
                   for (String c : a.categories) distinctCats.add(c);
                   for (String cat : distinctCats) {
                      if (targetCategories.contains(cat)) {
                         addToLocalMap(localCats, cat);
                      }
                   }
                }

                if ("english".equals(a.language)) {
                   processKeywords(a.text, localKeywords);
                }
             }

             myAllArticles = null;
             mergeToGlobalAtomic(localAuthors, authorCounts);
             mergeToGlobalAtomic(localLangs, langCounts);
             mergeToGlobalAtomic(localCats, catCounts);
             mergeToGlobalAtomic(localKeywords, keywordCounts);

             collectedResults.add(myValidArticles);
             uniqueArticlesCount.addAndGet(myValidArticles.size());

             barrier.await();

             if (id == 0) {
                writeAllOutputs();
             }

          } catch (Exception e) {
             e.printStackTrace();
          }
       }

       private void addToLocalMap(Map<String, Integer> map, String key) {
           Integer val = map.get(key);
           if (val == null) {
               map.put(key, 1);
           } else {
               map.put(key, val + 1);
           }
       }

       private void mergeToGlobalAtomic(Map<String, Integer> local, ConcurrentHashMap<String, AtomicInteger> global) {
           for (Map.Entry<String, Integer> entry : local.entrySet()) {
               String key = entry.getKey();
               int val = entry.getValue();

               AtomicInteger globalVal = global.get(key);
               if (globalVal == null) {
                   global.putIfAbsent(key, new AtomicInteger(0));
                   globalVal = global.get(key);
               }
               globalVal.addAndGet(val);
           }
       }

       private void processKeywords(String text, Map<String, Integer> localMap) {
          if (text == null) return;

          String[] tokens = SPLIT_PATTERN.split(text.toLowerCase());

          Set<String> foundWords = new HashSet<>();
          for (String t : tokens) {
             String word = t.replaceAll("[^a-z]", "");
             if (word.length() > 0 && !stopWords.contains(word)) {
                foundWords.add(word);
             }
          }

          for (String w : foundWords) {
             addToLocalMap(localMap, w);
          }
       }

       private void writeAllOutputs() throws IOException {
          List<Article> allArticles = new ArrayList<>();
          for (List<Article> list : collectedResults) {
             allArticles.addAll(list);
          }

          Collections.sort(allArticles, new Comparator<Article>() {
             @Override
             public int compare(Article a1, Article a2) {
                int res = a2.published.compareTo(a1.published);
                if (res == 0) return a1.uuid.compareTo(a2.uuid);
                return res;
             }
          });

          PrintWriter pw = new PrintWriter("all_articles.txt");
          for (Article a : allArticles) {
             pw.println(a.uuid + " " + a.published);
          }
          pw.close();

          for (String cat : targetCategories) {
             List<String> list = new ArrayList<>();
             for (Article a : allArticles) {
                if (a.categories != null && a.categories.contains(cat)) {
                   list.add(a.uuid);
                }
             }
             if (!list.isEmpty()) {
                Collections.sort(list);
                String name = cat.replace(",", "").replace(" ", "_").replaceAll("_+", "_") + ".txt";
                PrintWriter pwCat = new PrintWriter(name);
                for (String u : list) pwCat.println(u);
                pwCat.close();
             }
          }

          for (String lang : targetLanguages) {
             List<String> list = new ArrayList<>();
             for (Article a : allArticles) {
                if (lang.equals(a.language)) {
                   list.add(a.uuid);
                }
             }
             if (!list.isEmpty()) {
                Collections.sort(list);
                PrintWriter pwLang = new PrintWriter(lang + ".txt");
                for (String u : list) pwLang.println(u);
                pwLang.close();
             }
          }

          List<Map.Entry<String, AtomicInteger>> sortedKw = new ArrayList<>(keywordCounts.entrySet());
          Collections.sort(sortedKw, new Comparator<Map.Entry<String, AtomicInteger>>() {
             @Override
             public int compare(Map.Entry<String, AtomicInteger> e1, Map.Entry<String, AtomicInteger> e2) {
                int v1 = e1.getValue().get();
                int v2 = e2.getValue().get();
                if (v1 != v2) return v2 - v1;
                return e1.getKey().compareTo(e2.getKey());
             }
          });

          PrintWriter pwKw = new PrintWriter("keywords_count.txt");
          for (Map.Entry<String, AtomicInteger> e : sortedKw) {
             pwKw.println(e.getKey() + " " + e.getValue().get());
          }
          pwKw.close();

          PrintWriter pwRep = new PrintWriter("reports.txt");
          pwRep.println("duplicates_found - " + duplicatesFound.get());
          pwRep.println("unique_articles - " + uniqueArticlesCount.get());

          writeTop(pwRep, authorCounts, "best_author");
          writeTop(pwRep, langCounts, "top_language");

          String topCat = "";
          String topCatNorm = "";
          int maxC = -1;

          for (Map.Entry<String, AtomicInteger> e : catCounts.entrySet()) {
             int val = e.getValue().get();
             String norm = e.getKey().replace(",", "").replace(" ", "_").replaceAll("_+", "_");

             if (val > maxC) {
                maxC = val;
                topCat = e.getKey();
                topCatNorm = norm;
             } else if (val == maxC) {
                if (topCatNorm.equals("") || norm.compareTo(topCatNorm) < 0) {
                   topCat = e.getKey();
                   topCatNorm = norm;
                }
             }
          }
          if (maxC != -1) pwRep.println("top_category - " + topCatNorm + " " + maxC);

          if (!allArticles.isEmpty()) {
             Article mr = allArticles.get(0);
             pwRep.println("most_recent_article - " + mr.published + " " + mr.url);
          }

          if (!sortedKw.isEmpty()) {
             Map.Entry<String, AtomicInteger> top = sortedKw.get(0);
             pwRep.println("top_keyword_en - " + top.getKey() + " " + top.getValue().get());
          }
          pwRep.close();
       }

       private void writeTop(PrintWriter pw, Map<String, AtomicInteger> map, String label) {
          String best = "";
          int max = -1;
          for (Map.Entry<String, AtomicInteger> e : map.entrySet()) {
             int val = e.getValue().get();
             if (val > max) {
                max = val;
                best = e.getKey();
             } else if (val == max) {
                if (best.equals("") || e.getKey().compareTo(best) < 0) {
                   best = e.getKey();
                }
             }
          }
          if (max != -1) pw.println(label + " - " + best + " " + max);
       }
    }

    private static void readAuxiliaryFiles(String file) throws IOException {
       File f = new File(file);
       if (!f.exists()) return;

       File parentDir = f.getParentFile();
       List<String> lines = Files.readAllLines(f.toPath());
       if (lines.size() < 4) return;

       readSet(parentDir, lines.get(1), targetLanguages, false);
       readSet(parentDir, lines.get(2), targetCategories, false);
       readSet(parentDir, lines.get(3), stopWords, true);
    }

    private static void readSet(File parentDir, String line, Set<String> target, boolean lower) throws IOException {
       String path = line.trim();
       File f = (parentDir != null) ? new File(parentDir, path) : new File(path);

       if (!f.exists()) return;

       List<String> lines = Files.readAllLines(f.toPath());
       for (int i = 1; i < lines.size(); i++) {
          String s = lines.get(i).trim();
          if (lower) s = s.toLowerCase();
          target.add(s);
       }
    }

    private static void readInputFilesList(String file) throws IOException {
       File f = new File(file);
       if (!f.exists()) return;

       File parentDir = f.getParentFile();
       List<String> lines = Files.readAllLines(f.toPath());
       for (int i = 1; i < lines.size(); i++) {
          String relativePath = lines.get(i).trim();
          File actualFile = (parentDir != null) ? new File(parentDir, relativePath) : new File(relativePath);
          inputFiles.add(actualFile.getPath());
       }
    }
}