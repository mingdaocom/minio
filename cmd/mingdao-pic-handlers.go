package cmd

import (
	"encoding/base64"
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"image/png"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/disintegration/imaging"
	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	"github.com/rwcarlsen/goexif/exif"
	"golang.org/x/image/font"
	"golang.org/x/image/math/fixed"
	"gopkg.in/gographics/imagick.v2/imagick"
)

type MingImageFormat struct {
	vframe            string
	vframeoffset      int
	vframeh           string
	vframew           string
	rotate            string
	interlace         int
	imageView2        string
	format            string
	ignoreError       int
	w                 string
	h                 string
	q                 int
	imageMogr2        bool
	autoorient        bool
	state             bool
	watermark         string
	watermarktext     string
	watermarkfontsize int
	watermarkfont     string
	watermarkfill     string
	watermarkgravity  string
}

func formatImageUrl(mingformat *MingImageFormat, key string) {
	formatInfo := strings.Split(key, "/")
	switch formatInfo[0] {
	case "vframe":
		mingformat.vframe = formatInfo[1]
		mingformat.format = formatInfo[1]
		mingformat.state = true
		for i, v := range formatInfo {
			if v == "offset" {
				mingformat.vframeoffset, _ = strconv.Atoi(formatInfo[i+1])
			}
			if v == "rotate" {
				mingformat.rotate = formatInfo[i+1]
			}
			if v == "h" {
				mingformat.vframeh = formatInfo[i+1]
			}
			if v == "w" {
				mingformat.vframew = formatInfo[i+1]
			}
		}

	case "imageView2":
		mingformat.imageView2 = formatInfo[1]
		mingformat.state = true
		for i, v := range formatInfo {
			if v == "w" {
				mingformat.w = formatInfo[i+1]
			}
			if v == "h" {
				mingformat.h = formatInfo[i+1]
			}
			if v == "q" {
				mingformat.q, _ = strconv.Atoi(formatInfo[i+1])
			}
			if v == "format" {
				mingformat.format = formatInfo[i+1]
			}
			if v == "ignore-error" {
				mingformat.ignoreError, _ = strconv.Atoi(formatInfo[i+1])
			}
			if v == "interlace" {
				mingformat.interlace, _ = strconv.Atoi(formatInfo[i+1])
			}
			if v == "format" {
				mingformat.format = formatInfo[i+1]
			}
		}
	case "imageMogr2":
		mingformat.imageMogr2 = true
		mingformat.state = true
		for i, v := range formatInfo {
			if v == "auto-orient" {
				mingformat.autoorient = true
			}
			if v == "interlace" {
				mingformat.interlace, _ = strconv.Atoi(formatInfo[i+1])
			}
			if v == "format" {
				mingformat.format = formatInfo[i+1]
			}
		}
	case "watermark":
		mingformat.watermark = formatInfo[1]
		mingformat.state = true
		for i, v := range formatInfo {
			if v == "text" {
				text, _ := base64.URLEncoding.DecodeString(formatInfo[i+1])
				mingformat.watermarktext = string(text)
			}
			if v == "font" {
				font, _ := base64.URLEncoding.DecodeString(formatInfo[i+1])
				mingformat.watermarkfont = string(font)
			}
			if v == "fill" {
				mingformat.watermarkfill = formatInfo[i+1]
			}
			if v == "gravity" {
				mingformat.watermarkgravity = formatInfo[i+1]
			}
			if v == "fontsize" {
				mingformat.watermarkfontsize, _ = strconv.Atoi(formatInfo[i+1])
			}
			if v == "format" {
				mingformat.format = formatInfo[i+1]
			}
		}
	default:
	}
}

func ReadOrientation(filename string) int {
	file, err := os.Open(filename)
	if err != nil {
		//fmt.Println("failed to open file, err: ", err)
		return 0
	}
	defer file.Close()

	x, err := exif.Decode(file)
	if err != nil {
		//fmt.Println("failed to decode file, err: ", err)
		return 0
	}

	orientation, err := x.Get(exif.Orientation)
	if err != nil {
		//fmt.Println("failed to get orientation, err: ", err)
		return 0
	}
	orientVal, err := orientation.Int(0)
	if err != nil {
		//fmt.Println("failed to convert type of orientation, err: ", err)
		return 0
	}

	//fmt.Println("the value of photo orientation is :", orientVal)
	return orientVal
}

func MingPicService(gr *GetObjectReader, format *MingImageFormat) error {
	//file := os.File{}

	if format.vframe != "" {
		err := GetVframe(gr, format)
		if err != nil {
			return err
		}
		gr.ObjInfo.path = gr.tmppath
		defer os.Remove(gr.tmppath)
	}

	var src image.Image
	var err error
	var gifType bool
	//把这个也删了
	//frameTmpPath:=gr.tmppath
	//if frameTmpPath!=""{
	//	defer os.Remove(frameTmpPath)
	//}
	fileSuffix := path.Ext(gr.ObjInfo.Name)

	if fileSuffix == ".gif" && format.format == "" && format.imageView2 == "" {
		gifType = true
	} else if gr.tmppath != "" {
		src, err = imaging.Open(gr.tmppath)
		fileSuffix = path.Ext(gr.tmppath)
		//读取之后删了这个文件
		tmpPathOld := gr.tmppath
		defer os.Remove(tmpPathOld)
	} else {
		src, _, err = image.Decode(gr)
		//src, err = imaging.Open(gr.ObjInfo.path, imaging.AutoOrientation(true))
	}

	dst := &image.NRGBA{}
	if format.imageView2 == "" && gifType == false {
		fmt.Println("resize start")
		weight := src.Bounds().Dx()
		high := src.Bounds().Dy()
		dst = imaging.Resize(src, weight, high, imaging.Lanczos)
		fmt.Println("resize end")


	} else if gifType == false {
		weight, _ := strconv.Atoi(format.w)
		//if err != nil {
		//	error := ErrorCode{1, "weight parse error"}
		//return &error
		//}
		high, _ := strconv.Atoi(format.h)
		//if err != nil {
		//	error := ErrorCode{1, "high parse error"}
		//return &error
		//}

		srcDx := src.Bounds().Dx()
		srcDy := src.Bounds().Dy()

		//mode2
		switch format.imageView2 {
		case "0":
			//mode0
			if high == 0 {
				dst = imaging.Resize(src, weight, 0, imaging.Lanczos)
			} else if weight == 0 {
				dst = imaging.Resize(src, 0, high, imaging.Lanczos)
			} else if srcDx > srcDy {
				if float64(srcDx)/float64(srcDy) > float64(weight)/float64(high) {
					dst = imaging.Resize(src, weight, 0, imaging.Lanczos)
				} else {
					dst = imaging.Resize(src, 0, high, imaging.Lanczos)
				}
			} else {
				if float64(srcDx)/float64(srcDy) > float64(weight)/float64(high) {
					dst = imaging.Resize(src, 0, weight, imaging.Lanczos)
				} else {
					dst = imaging.Resize(src, high, 0, imaging.Lanczos)
				}
			}
		case "1":
			if weight == 0 {
				weight = high
			} else if high == 0 {
				high = weight
			}
			if float64(srcDx)/float64(srcDy) > float64(weight)/float64(high) {
				src = imaging.Resize(src, 0, high, imaging.Lanczos)
			} else {
				src = imaging.Resize(src, weight, 0, imaging.Lanczos)
			}
			dst = imaging.CropAnchor(src, weight, high, imaging.Center)
		case "2":
			if high == 0 {
				dst = imaging.Resize(src, weight, 0, imaging.Lanczos)
			} else if weight == 0 {
				dst = imaging.Resize(src, 0, high, imaging.Lanczos)
			} else if float64(srcDx)/float64(srcDy) > float64(weight)/float64(high) {
				dst = imaging.Resize(src, weight, 0, imaging.Lanczos)
			} else {
				dst = imaging.Resize(src, 0, high, imaging.Lanczos)
			}
		}
	}
	//now := time.Now().Unix()
	rand.Seed(time.Now().UnixNano())
	gr.tmppath = globalTmpPrefix + "tmp/" + strconv.Itoa(int(rand.Int())) + "_" + mustGetUUID() + fileSuffix
	//gr.path="tmp/"+gr.ObjInfo.Name
	//out, err := os.Create(gr.path)

	if gifType == true {
		//inputFile, err := os.Open(gr.ObjInfo.path)
		//if err != nil {
		//	error := ErrorCode{1, "read file error"}
		//	return &error
		//}
		//defer inputFile.Close()
		file, err := os.Create(gr.tmppath)
		defer file.Close()
		if err != nil {
			error := ErrorCode{1, "create file error"}
			return &error
		}
		_, err = io.Copy(file, gr)
		if err != nil {
			error := ErrorCode{1, "copy file error"}
			return &error
		}
	} else {
		if format.imageView2 == "" {
			fmt.Println("start save")
			err = imaging.Save(dst, gr.tmppath)
			fmt.Println("end  save")
		} else {
			err = imaging.Save(dst, gr.tmppath, imaging.JPEGQuality(format.q))
		}

		if err != nil {
			fmt.Println(err)
			fmt.Println(err.Error())
			error := ErrorCode{1, "create file error"}
			return &error
		}

		if format.interlace == 1 || format.format != "" {
			imagickImage(gr.tmppath, format.format)
		}
	}

	gr.ObjInfo.Size = GetFileSize(gr.tmppath)
	if gr.ObjInfo.Size == 0 {
		fmt.Println("imaging !!!!", err)

	}

	return nil
}

//画水印
type DrawText struct {
	JPG    draw.Image
	Merged *os.File
	Src    image.Image

	Title string
	X0    int
	Y0    int
	Size0 float64

	SubTitle string
	X1       int
	Y1       int
	Size1    float64
}

func DrawPoster(d *DrawText, fontName string, ext string) error {
	fontSource := "./font/" + fontName
	fontSourceBytes, err := ioutil.ReadFile(fontSource)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	trueTypeFont, err := freetype.ParseFont(fontSourceBytes)
	if err != nil {
		return err
	}

	draw := &font.Drawer{
		Dst: d.JPG,
		Src: d.Src,
		Face: truetype.NewFace(trueTypeFont, &truetype.Options{
			Size:    d.Size0,
			DPI:     72,
			Hinting: font.HintingNone,
		}),
	}

	y := (d.JPG.Bounds().Dy()-int(math.Ceil(d.Size0)))/2 + int(math.Ceil(d.Size0)) - 5
	//y := (fixed.I( - int(math.Ceil(d.Size0))))/2

	//fmt.Println("y  =   ",y," fixed",fixed.I(int(y)))
	//fmt.Println("x  =  ",(fixed.I(d.JPG.Bounds().Dx()) - draw.MeasureString(d.Title)) / 2)
	//dy := int(math.Ceil(d.Size0))
	draw.Dot = fixed.Point26_6{
		X: (fixed.I(d.JPG.Bounds().Dx()) - draw.MeasureString(d.Title)) / 2,
		Y: fixed.I(int(y)),
	}
	draw.DrawString(d.Title)

	//fc := freetype.NewContext()
	//fc.SetDPI(72)
	//fc.SetFont(trueTypeFont)
	//fc.SetFontSize(d.Size0)
	//fc.SetClip(d.JPG.Bounds())
	//fc.SetDst(d.JPG)
	//fc.SetSrc(d.Src)
	//
	//pt := freetype.Pt(d.X0,d.Y0 + int(fc.PointToFixed(d.Size0)*3/2) >> 8)
	//_, err = fc.DrawString(d.Title, pt)
	//if err != nil {
	//	return err
	//}

	//fc.SetFontSize(d.Size1)
	//_, err = fc.DrawString(d.SubTitle, freetype.Pt(d.X1, d.Y1))
	//if err != nil {
	//	return err
	//}
	if ext == "jpeg" {
		err = jpeg.Encode(d.Merged, d.JPG, nil)

		if err != nil {
			return err
		}
	} else {
		err = png.Encode(d.Merged, d.JPG)

		if err != nil {
			return err
		}
	}
	return nil
}

func MingWaterMark(gr *GetObjectReader, format *MingImageFormat) error {
	//file, err := os(gr)
	//if err != nil {
	//	error := ErrorCode{1, "open file error"}
	//	return &error
	//}
	//defer file.Close()
	//fileSuffix := path.Ext(gr.ObjInfo.path)
	// decode jpeg into image.Image
	img, name, err := image.Decode(gr)

	b := img.Bounds()
	m := image.NewRGBA(b)
	draw.Draw(m, b, img, image.ZP, draw.Src)
	now := time.Now().Unix()
	fileSuffix := path.Ext(gr.ObjInfo.Name)
	gr.tmppath = globalTmpPrefix + "tmp/" + strconv.Itoa(int(now)) + "_" + mustGetUUID() + fileSuffix
	out, err := os.Create(gr.tmppath)
	if err != nil {
		error := ErrorCode{1, "create file error"}
		return &error
	}
	defer out.Close()
	textsize := utf8.RuneCountInString(format.watermarktext)

	X0 := (b.Dx() - (format.watermarkfontsize/20)*textsize) / 2
	Y0 := b.Dy() / 2

	err = DrawPoster(&DrawText{
		JPG:    m,
		Merged: out,
		Title:  format.watermarktext,
		X0:     X0,
		Y0:     Y0,
		Size0:  float64(format.watermarkfontsize / 20),
		Src:    image.White,
	}, "MSYH.TTC", name)

	if err != nil {
		error := ErrorCode{1, "watermark file error"}
		return &error
	}
	gr.ObjInfo.Size = GetFileSize(gr.tmppath)
	return nil
}

func GetVframe(gr *GetObjectReader, format *MingImageFormat) error {
	//offset:=strconv.Itoa(format.vframeoffset)
	rand.Seed(time.Now().UnixNano())

	offset := time.Unix(int64(format.vframeoffset), 0).Format("00:04:05")

	gr.tmppath = globalTmpPrefix + "tmp/" + strconv.Itoa(int(rand.Int())) + "_" + gr.ObjInfo.ETag + "." + format.vframe

	cmd := exec.Command("ffmpeg", "-i", gr.ObjInfo.path, "-y", "-f", "image2", "-ss", offset, "-t", "0.001", gr.tmppath)
	//cmd := exec.Command("ffmpeg", "-i", gr.ObjInfo.path, "-vframes", strconv.Itoa(1), "-s", fmt.Sprintf("%dx%d", 100, 200), "-f", "singlejpeg", "-")

	//cmd.Run()
	e := cmd.Run()
	if e != nil {
		error := ErrorCode{1, "getvframe  error"}
		return &error
	}
	return nil
}

func GetFileSize(filename string) int64 {
	var result int64
	filepath.Walk(filename, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			fmt.Println("get size error!!!!! ", err)
			result = 0
		} else {
			result = f.Size()
		}
		return nil
	})
	return result
}

func imagickImage(filepath string, format string) error {
	fmt.Println("start imagick ")
	imagick.Initialize()
	defer imagick.Terminate()
	mw := imagick.NewMagickWand()

	mw.ReadImage(filepath)
	mw.SetInterlaceScheme(imagick.INTERLACE_PLANE)
	if format != "" {
		mw.SetImageFormat(format)
	}
	mw.WriteImage(filepath)
	mw.Clear()
	mw.ReadImage(filepath)
	fmt.Println("end imagick ")

	return nil
}
