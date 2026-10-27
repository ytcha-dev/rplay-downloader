import {
  EncodedAudioPacketSource, EncodedPacketSink, HLS_FORMATS,
  Input, Mp4OutputFormat, Output, StreamTarget, UrlSource,
} from 'mediabunny';
import { FETCH_PARALLELISM, SOURCE_CACHE_SIZE, TARGET_CHUNK_SIZE } from './download-config.js';
import { RemuxCompatibilityError, SourceError, TaskCanceledError, isAbortError } from './errors.js';
import { createTrackedFetch } from './network.js';
import { createTempFile, removeTempFile } from './opfs.js';
import { TaskPhase } from './protocol.js';
import { openDashInputs } from './dash-input.js';

/**
 * Export an independently addressed audio rendition without fetching video media segments.
 * Combined A/V streams are deliberately unsupported in this mode.
 */
export async function remuxAudioOnly(task, reporter, context) {
  if (!task.audioUrl || task.audioUrl === task.streamUrl) {
    throw new SourceError('来源没有独立音轨，无法仅下载音频', 'NO_SEPARATE_AUDIO');
  }
  const temp = await createTempFile(task.taskId, 'm4a');
  const trackedFetch = createTrackedFetch(reporter, context.controller);
  let input = null;
  try {
    let timestampOffset = 0;
    let duration = task.duration || null;
    if (task.sourceType === 'dash') {
      const dash = await openDashInputs(task, trackedFetch, context);
      input = dash.audioInput;
      timestampOffset = dash.audioTimestampOffset || 0;
      duration = dash.duration || duration;
    } else {
      input = new Input({
        formats: HLS_FORMATS,
        source: new UrlSource(task.audioUrl, {
          requestInit: { credentials: 'include' },
          parallelism: FETCH_PARALLELISM,
          maxCacheSize: SOURCE_CACHE_SIZE,
          fetchFn: trackedFetch,
        }),
        formatOptions: { hls: { offsetTimestampsByDateTime: false } },
      });
    }
    if (!input || !(await input.canRead())) throw new SourceError('无法读取独立音轨', 'UNREADABLE_AUDIO');
    context.audioInput = input;
    const audioTracks = await input.getAudioTracks();
    if (audioTracks.length !== 1) throw new SourceError('独立音轨数量异常', 'INVALID_AUDIO_TRACK');
    const track = audioTracks[0];
    if (await track.isLive()) throw new SourceError('仅支持 VOD 音频', 'LIVE_UNSUPPORTED');
    const codec = await track.getCodec();
    const format = new Mp4OutputFormat({ fastStart: false });
    if (!codec || !format.getSupportedCodecs().includes(codec)) {
      throw new RemuxCompatibilityError(`M4A 不支持音频编码 ${codec || 'UNKNOWN'}`);
    }
    const sink = new EncodedPacketSink(track);
    const first = await sink.getFirstPacket();
    const decoderConfig = await track.getDecoderConfig();
    if (!first || !decoderConfig) throw new SourceError('独立音轨没有可用音频数据', 'EMPTY_AUDIO_TRACK');
    if (!duration) duration = await input.getDurationFromMetadata([track]);
    const writable = await temp.handle.createWritable({ keepExistingData: false });
    const output = new Output({
      format,
      target: new StreamTarget(writable, { chunked: true, chunkSize: TARGET_CHUNK_SIZE }),
    });
    context.output = output;
    const source = new EncodedAudioPacketSource(codec);
    output.addAudioTrack(source, { languageCode: await track.getLanguageCode() });
    output.setMetadataTags({ title: task.title });
    reporter.setPhase(TaskPhase.REMUXING, '正在无转码封装 M4A…');
    await output.start();
    const baseTimestamp = first.timestamp;
    for await (const packet of sink.packets(first)) {
      if (context.controller.signal.aborted) throw new TaskCanceledError();
      const normalized = packet.clone({ timestamp: packet.timestamp - baseTimestamp });
      await source.add(normalized, { decoderConfig });
      reporter.notePacket(normalized.timestamp + normalized.duration, duration);
    }
    source.close();
    await output.finalize();
    context.output = null;
    const file = await temp.handle.getFile();
    if (!file.size) throw new RemuxCompatibilityError('M4A 输出文件为空');
    return { ...temp, size: file.size };
  } catch (error) {
    if (context.output && !['canceled', 'finalized'].includes(context.output.state)) {
      await context.output.cancel().catch(() => {});
    }
    await removeTempFile(temp);
    if (isAbortError(error) || context.controller.signal.aborted) throw new TaskCanceledError();
    throw error;
  } finally {
    input?.dispose();
    context.input = null;
    context.audioInput = null;
    context.output = null;
    context.prefetcher?.dispose();
    context.prefetcher = null;
  }
}
