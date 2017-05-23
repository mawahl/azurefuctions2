#r "Microsoft.WindowsAzure.Storage"
#r "Newtonsoft.Json"

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;

static Queue<string> BLOB_QUEUE = new Queue<string>();
static Queue<string> BLOB_DELETE_QUEUE = new Queue<string>();
static string ROOT = "D:\\home\\site\\wwwroot\\EncodeBlob_FFMPEG\\";
static string MP4_AUDIO_HEADER = ROOT+ "blobs\\header";
static string BLOB_CACHE_PATH =  ROOT+ "blobs\\";
static string FFMPEG_PATH = ROOT + "bin\\ffmpeg\\ffmpeg.exe";
static string FFMPEG_ARGUMENTS = "-vn -acodec pcm_s16le -ar 16000 -ac 1";

static int maxQueueLength = 3;
static int totalFragments = 0;
static string lastFragBlob;


public static void Run(CloudBlockBlob inputBlob, string fileName, CloudBlockBlob outputBlob, TraceWriter log)
{
    log.Info("VERSION: 11-15.1025");

    log.Info($"[{fileName}] New FragBlob: {fileName}");
    log.Info($"[{fileName}] Last Frag Blob: " + lastFragBlob);

    StringBuilder outputCopyCommand = new StringBuilder();
    StringBuilder outputFFMPEG = new StringBuilder();

    string blobFilePath = BLOB_CACHE_PATH + "input\\" + fileName;

    try
    {
        log.Info($"[{fileName}]  Downloading Blob: {blobFilePath}");
        //Download the FragBlob
        inputBlob.DownloadToFile(blobFilePath, FileMode.Create);
        log.Info($"[{fileName}] Download Succeeded.");

        //Add Blob to the Queue
        BLOB_QUEUE.Enqueue(fileName);
        log.Info($"[{fileName}] Blob Queue Count: {BLOB_QUEUE.Count}");
        
        // Wait for the maxQueueLength number of blobs to be in the queue. 
        // We will concatenate them into a single MP4 and convert to WAV prior to writing out.
        if (BLOB_QUEUE.Count >= maxQueueLength)
        {
            string outputMP4 = BLOB_CACHE_PATH + "merged\\"+ fileName + "_merged.mp4";
            string copyArguments =  "/c copy /B " + MP4_AUDIO_HEADER;

            for (int i = 0; i < maxQueueLength; i++)
            { 
                copyArguments+= "+";
                string mergeBlob = BLOB_QUEUE.Dequeue();
                BLOB_DELETE_QUEUE.Enqueue(mergeBlob);
                copyArguments+= BLOB_CACHE_PATH + "input\\"+ mergeBlob;
            }
            copyArguments += " " + outputMP4;

            log.Info($"[{fileName}] Copy: cmd {copyArguments}");
 
            Process copyCommand = new Process();
            copyCommand.StartInfo.UseShellExecute = false;
            copyCommand.StartInfo.FileName = "cmd";
            copyCommand.StartInfo.CreateNoWindow = true;
            copyCommand.StartInfo.Arguments = copyArguments;
            copyCommand.StartInfo.RedirectStandardOutput = true;
            copyCommand.StartInfo.RedirectStandardError = true;
            copyCommand.OutputDataReceived += new DataReceivedEventHandler((sender, e) =>
            {
                // Prepend line numbers to each line of the output.
                if (!String.IsNullOrEmpty(e.Data))
                {
                    outputCopyCommand.Append("\n Copy:" + e.Data);
                }
            });
            copyCommand.ErrorDataReceived += new DataReceivedEventHandler((sender, e) =>
            {
                // Prepend line numbers to each line of the output.
                if (!String.IsNullOrEmpty(e.Data))
                {
                    outputCopyCommand.Append("\n Copy:" + e.Data);
                }
            });
            copyCommand.Start();
            copyCommand.BeginOutputReadLine();
            copyCommand.BeginErrorReadLine();
            copyCommand.WaitForExit();
                
            // Enabling this line will show you lots of error details from FFMPEG> 
            log.Info($"[{fileName}] COPY Details:" + outputCopyCommand.ToString()); 
            log.Info($"[{fileName}] COPY MERGE OUTPUT : \n" + outputMP4);
            log.Info($"[{fileName}] Blob Delete Queue Count :{BLOB_DELETE_QUEUE.Count()}");

            // Delete the blobs in the queue
            var deleteQueueCount = BLOB_DELETE_QUEUE.Count(); 
            for (int i = 0; i < deleteQueueCount; i++)
            {
                string deleteBlob = BLOB_DELETE_QUEUE.Dequeue();
                FileInfo blob = new FileInfo(BLOB_CACHE_PATH + "input\\" + deleteBlob);
                blob.Delete();
                log.Info($"[{fileName}]: Deleted input {blob.FullName}");
            }

            log.Info($"[{fileName}]: Starting FFMPEG...");
            string ffmpegArguments = FFMPEG_ARGUMENTS;
            string ffmpegInput = "-i " + outputMP4;
            string ffmpegOutput = BLOB_CACHE_PATH +"wav\\" + fileName + ".wav";
            
            log.Info($"[{fileName}]: FFMPEG command: "+ ffmpegInput + " " + ffmpegArguments + " " + ffmpegOutput);

            Process ffmpegCommand = new Process();
            ffmpegCommand.StartInfo.UseShellExecute = false;
            ffmpegCommand.StartInfo.FileName = FFMPEG_PATH;
            ffmpegCommand.StartInfo.CreateNoWindow = true;
            ffmpegCommand.StartInfo.Arguments = ffmpegInput + " " + ffmpegArguments + " " + ffmpegOutput;
            ffmpegCommand.StartInfo.RedirectStandardOutput = true;
            ffmpegCommand.StartInfo.RedirectStandardError = true;
            ffmpegCommand.OutputDataReceived += new DataReceivedEventHandler((sender, e) =>
            {
                // Prepend line numbers to each line of the output.
                if (!String.IsNullOrEmpty(e.Data))
                {
                    outputFFMPEG.Append("\nFFMPEG:" + e.Data);
                }
            });
            ffmpegCommand.ErrorDataReceived += new DataReceivedEventHandler((sender, e) =>
            {
                // Prepend line numbers to each line of the output.
                if (!String.IsNullOrEmpty(e.Data))
                {
                    outputFFMPEG.Append("\nFFMPEG:" + e.Data);
                }
            });

            ffmpegCommand.Start();
            // Asynchronously read the standard output of the spawned process. 
            // This raises OutputDataReceived events for each line of output.
            ffmpegCommand.BeginOutputReadLine();
            ffmpegCommand.BeginErrorReadLine();
            ffmpegCommand.WaitForExit();
                
            // Enabling this line will show you lots of error details from FFMPEG> 
            log.Info($"[{fileName}] FFMPEG Details:" + outputFFMPEG.ToString());
    
            FileInfo ouputWavFileInfo = new FileInfo(ffmpegOutput);
            FileInfo outputMergedMP4 = new FileInfo(outputMP4);
            CopyToOutput(ouputWavFileInfo, outputBlob).Wait();
            // Change some settings on the output blob.
            //outputBlob.Metadata["Custom1"] = "Some Custom Metadata";
            outputBlob.Properties.ContentType = "audio/wav";
            outputBlob.SetProperties();
            // Cleanup the merged MP4 and WAV files on local disk
            ouputWavFileInfo.Delete();
            outputMergedMP4.Delete();

            log.Info($"[{fileName}] Uploaded Output: {ffmpegOutput}");
            
        }
        else{
            log.Info($"[{fileName}] Not enough Fragments yet... waiting for {maxQueueLength - BLOB_QUEUE.Count} more in the Queue!");
        }

        log.Info($"[{fileName}] Done!");

        //Set the last known fragment name and increment the total fragment count.
        lastFragBlob = fileName;
        totalFragments++;
    }
    catch (Exception ex)
    {
        log.Error($"[{fileName}] : ERROR: failed.");
        log.Info($"[{fileName}]: StackTrace : {ex.StackTrace}");
        throw ex;
    }
}

private static async Task CopyToOutput(FileInfo fileInfo, CloudBlockBlob destination)
{  
        using (var stream = fileInfo.OpenRead())
        {
            await destination.UploadFromStreamAsync(stream);
        }
}