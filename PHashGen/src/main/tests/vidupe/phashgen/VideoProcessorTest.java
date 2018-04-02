package vidupe.phashgen;

import com.google.api.services.drive.Drive;
import org.junit.Test;
import vidupe.message.HashGenMessage;

import static junit.framework.TestCase.assertTrue;

public class VideoProcessorTest {
    String hashes = "1010110110011001011010010010000110111000010001000011000001011010$1011100010001001011011010010011001100001001100000001000011000011$1011100111000011001001000011100100110000100001000010000101100100$1110100110011000001010000010011001100100000000100010001101001100$1110110011001111001010000011000011100011100100110011000000110010$1011100110001110011010010011000100100100010011000000100100110000$1011100100111001011001110100110100111000100101100000010001001001$1010110000101001011001111001001010111000001001000010000101100001$1011100010011001011010011001001001101100011011000001001001100000$1010110100101100100010000001001101101100110011000000011001000000$";


    @Test
    public void generatePhash() {
        VideoProcessor videoProcessor = new VideoProcessor();
        String accessToken = "ya29.Glt7BanDIhHKwGBfkXzfYiiY6VabSDevaQIYYIDRd4YYnL5nUDCWZs27SuBRT1Ijrqv8A9schQ-Rx11JK3XxItvig3nTslQGuAMOxt2NHIWO7TS4vILZxWyoQ55i";
        HashGenMessage message = HashGenMessage.builder().accessToken(accessToken).videoId("0B-cyY07ful39VDBYQkxvZmRuek0")
                .videoName("video.mp4").email("").build();
        final Drive drive = videoProcessor.getDrive(message);
        assertTrue("drive returned null",drive!=null);
    }

    @Test
    public void processVideo() {
        VideoProcessor videoProcessor = new VideoProcessor();
        String accessToken = "ya29.GluOBb6XTAOOXZthkQyYlWdnJWTo8Eha93GGp6weWiu-wK2gnw7GuSNoM8IRFymnA7QHeB0SDWAOUnmVkY6p6vUrt3jzJREwDb9_C-YXA0iCI5m_q5y0cmByJaW2 ";
        HashGenMessage message = HashGenMessage.builder().accessToken(accessToken).videoId("1IjBu9EIRppivyrd_b7g8PqnikCBznhPy")
                .videoName("Copy of video.mp4").email("").build();
        final Drive drive = videoProcessor.getDrive(message);
        VideoAudioHashes hashes = videoProcessor.processVideo(message, drive);
        System.out.println(hashes.getVideoHashes());
        assertTrue("hashes are null", hashes.getVideoHashes() != null);
    }
}