package kafka.reading.select;

import java.util.Arrays;
import java.util.Random;

public class PartitionerSelect {

    public static void main(String[] args) {
        int[] cumulativeFrequencyTable = new int[]{4, 0, 8}; // 累积率表
        int[] partitionIds             = new int[]{0, 1, 2}; // 分区id列表
        int   random                   = new Random().nextInt(3);

        int partitionLoadStats = 3;
        int weightedRandom     = random % cumulativeFrequencyTable[8 - 1];
        System.out.println("weightedRandom" + weightedRandom);
        int searchResult   = Arrays.binarySearch(cumulativeFrequencyTable, 0, 3, weightedRandom);
        int partitionIndex = Math.abs(searchResult + 1);
        // 映射到真实分区ID
        System.out.println("分区号" + partitionIndex);

    }
}