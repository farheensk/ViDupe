package vidupe.dedupe;

public class ComputeHammingDistance {
    private int size = 32;
    private int smallerSize = 8;
    private double[] c;

// Returns a 'binary string' (like. 001010111011100010) which is easy to do a hamming hammingDistance on.

    public int hammingDistance(String s1, String s2) {
        int counter = 0;
        for (int k = 0; k < s1.length(); k++) {
            if (s1.charAt(k) != s2.charAt(k)) {
                counter++;
            }
        }
        return counter;
    }
}
