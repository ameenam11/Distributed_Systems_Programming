import java.util.concurrent.ConcurrentLinkedQueue;

class AppsProcessingState {
    private int remainingLines;
    private ConcurrentLinkedQueue<String> processedLines;

    public AppsProcessingState(int numLines) {
        this.remainingLines = numLines;
        this.processedLines = new ConcurrentLinkedQueue<>();
    }

    public synchronized void decrementLineCount() {
        remainingLines--;
    }

    public synchronized boolean isComplete() {
        return remainingLines == 0;
    }

    public void addProcessedLine(String line) {
        processedLines.add(line);
    }

    public ConcurrentLinkedQueue<String> getProcessedLines() {
        return processedLines;
    }
}
