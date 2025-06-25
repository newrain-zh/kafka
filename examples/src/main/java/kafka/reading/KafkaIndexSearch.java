package kafka.reading;

import java.util.*;

/**
 * 模拟通过 offset 查找日志
 */
public class KafkaIndexSearch {

    // 模拟的 IndexEntry 结构
    static class IndexEntry {
        int  offset;  // 相对 baseOffset
        long position;       // 在 .log 文件中的字节偏移

        public IndexEntry(int offset, long position) {
            this.offset   = offset;
            this.position = position;
        }

        @Override
        public String toString() {
            return "[relativeOffset=" + offset + ", position=" + position + "]";
        }
    }

    public static void main(String[] args) {
        long baseOffset = 2000; // 每个 .index 文件都有对应的日志段起始 offset

        // 模拟 Kafka .index 文件中存储的索引项（稀疏）
        List<IndexEntry> indexEntries = Arrays.asList(
                new IndexEntry(0, 0),
                new IndexEntry(5, 1200),
                new IndexEntry(10, 2600),
                new IndexEntry(20, 5300)
        );

        long targetOffset = 2018; // 你要查找的 offset

        IndexEntry result = searchClosestIndexEntry(indexEntries, baseOffset, targetOffset);

        if (result != null) {
            long absoluteOffset = baseOffset + result.offset;
            System.out.println("在 index 中找到：offset <= " + targetOffset + ", entry: " + result +
                    "（实际 offset: " + absoluteOffset + "）");
            System.out.println("→ 应该从 position = " + result.position + " 的 .log 文件中开始扫描");
        } else {
            System.out.println("未找到适配的索引项");
        }
    }

    // 二分查找：找出 ≤ targetOffset 的最大 offset 的那条索引项
    public static IndexEntry searchClosestIndexEntry(List<IndexEntry> index, long baseOffset, long targetOffset) {
        int left = 0, right = index.size() - 1;
        IndexEntry result = null;

        while (left <= right) {
            int mid = (left + right) / 2;
            long midOffset = baseOffset + index.get(mid).offset;

            if (midOffset == targetOffset) {
                return index.get(mid); // 精准命中
            } else if (midOffset < targetOffset) {
                result = index.get(mid); // 记录当前可能的候选
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return result;
    }
}