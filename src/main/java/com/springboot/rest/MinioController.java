package com.springboot.rest;

import com.springboot.utils.MinioUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.util.List;

/**
 * @author xiaomai
 * @description 文件操作接口
 * @date 2023/7/4 10:19
 */

@RestController
@RequestMapping("/minio")
@Api(tags = "文件操作接口")
@Slf4j
public class MinioController {

    @Value("${minio.bucketName}")
    private String bucketName;

    @Resource
    private MinioUtil minioUtil;

    @ApiOperation("文件上传")
    @PostMapping("/upload")
    public void uploadFile(@RequestParam("file") MultipartFile file, boolean rename) throws Exception {
        minioUtil.uploadFile(file, bucketName, rename);
    }

    @ApiOperation("下载一个文件")
    @GetMapping("/download")
    public void download(@RequestParam String fileName, HttpServletResponse response) throws Exception {
        // 返回文件流，也可以根据需要进行其他处理，如下载到本地等
        InputStream stream = minioUtil.downloadFile(bucketName, fileName);

        ServletOutputStream output = response.getOutputStream();
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + fileName);
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
        response.setCharacterEncoding("UTF-8");
        IOUtils.copy(stream, output);
    }

    @ApiOperation("获取全部文件")
    @GetMapping("/getFiles")
    public List<String> getAllFiles(@RequestParam String bucket) throws Exception {
        return minioUtil.listObjects(bucket);
    }

    @ApiOperation("删除一个文件")
    @DeleteMapping("/deleteFile")
    public void deleteFile(@RequestParam String bucket, @RequestParam String fileName) throws Exception {
        minioUtil.deleteObject(bucket, fileName);
    }

    @ApiOperation("获取文件信息")
    @GetMapping("/getObject")
    public String getObject(@RequestParam String bucket, @RequestParam String objectName) throws Exception {
        return minioUtil.getObject(bucket, objectName);
    }

    @ApiOperation("获取一个连接以供下载")
    @GetMapping("/getPresignedObjectUrl")
    public String getPresignedObjectUrl(@RequestParam String bucket,
                                        @RequestParam String objectName,
                                        @RequestParam Integer expires) throws Exception {
        return minioUtil.getPresignedObjectUrl(bucket, objectName, expires);
    }

    @ApiOperation("列出所有的桶")
    @GetMapping("/listBuckets")
    public List listBuckets() throws Exception {
        return minioUtil.listBuckets();
    }

    @ApiOperation("创建存储桶")
    @PostMapping("/createBucket")
    public void createBucket(@RequestParam String bucket) throws Exception {
        minioUtil.createBucket(bucket);
    }

    @ApiOperation("删除存储桶")
    @DeleteMapping("/deleteBucket")
    public void deleteBucket(@RequestParam String bucket) throws Exception {
        minioUtil.deleteBucket(bucket);
    }

    @PostMapping("/uploadBigFile")
    public String uploadBigFile(@RequestParam("file") MultipartFile file,
                                @RequestParam int sliceIndex,
                                @RequestParam int totalPieces,
                                @RequestParam String fileName,
                                @RequestParam String md5) throws Exception {
        return minioUtil.uploadBigFile(file, bucketName, fileName, sliceIndex, totalPieces, md5);
    }


}