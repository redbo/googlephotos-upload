package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const defaultWorkers = 4
const imageFingerprintSize = 8192
const tokFile = ".googleuploads-token.json"

var imageExtensions = map[string]bool{
	".jpg":  true,
	".jpeg": true,
	".gif":  true,
	".heif": true,
	".orf":  true,
	".png":  true,
	".bmp":  true,
	".tiff": true,
	".tif":  true,
}

func main() {
	homedir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal("Unable to find home directory: ", err)
	}

	jsonConfig, err := ioutil.ReadFile("./credentials.json")
	if err != nil {
		log.Fatal("Loading creds: ", err)
	}
	config, err := google.ConfigFromJSON(jsonConfig,
		"https://www.googleapis.com/auth/photoslibrary")
	if err != nil {
		log.Fatal("Reading json config: ", err)
	}

	ctx := context.Background()

	var token *oauth2.Token

	if f, err := os.Open(filepath.Join(homedir, tokFile)); err == nil {
		json.NewDecoder(f).Decode(&token)
	}

	if token == nil {
		fmt.Println("Visit this link:", config.AuthCodeURL(""))

		fmt.Print("Enter token: ")
		var authCode string
		if _, err := fmt.Scan(&authCode); err != nil || len(authCode) == 0 {
			log.Fatal("Error reading authorization code: ", err)
		}

		token, err = config.Exchange(ctx, authCode)
		if err != nil {
			log.Fatal("Error getting token: ", err)
		}
		f, err := os.OpenFile(filepath.Join(homedir, tokFile), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			log.Fatal("Error saving token: ", err)
		}
		json.NewEncoder(f).Encode(token)
		f.Close()
	}

	client := config.Client(ctx, token)

	db, err := sql.Open("sqlite3", filepath.Join(homedir, ".googlephotos-uploader.db"))
	if err != nil {
		log.Fatal("Error opening database: ", err)
	}
	defer db.Close()
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS uploads (fingerprint TEXT PRIMARY KEY, filename TEXT, uploaded INTEGER);")
	if err != nil {
		log.Fatal("Error creating table: ", err)
	}

	jobs := make(chan string, 100)
	wg := &sync.WaitGroup{}
	wrkPtr := flag.Int("c", defaultWorkers, "number of concurrent uploads")
	flag.Parse()
	for i := 0; i < *wrkPtr; i++ {
		go func() {
			for path := range jobs {
				uploadFile(path, token, client, db)
				wg.Done()
			}
		}()
	}
	for _, dir := range flag.Args() {
		fmt.Println(dir)
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if imageExtensions[strings.ToLower(filepath.Ext(path))] {
				fmt.Println("...", path)
				wg.Add(1)
				jobs <- path
			}
			return nil
		})
	}
	close(jobs)
	wg.Wait()
}

func uploadFile(filename string, token *oauth2.Token, client *http.Client, db *sql.DB) error {
	upload, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("Error opening file: %v", err)
	}
	defer upload.Close()

	h := sha256.New()
	if _, err := io.CopyN(h, upload, imageFingerprintSize); err != nil {
		return fmt.Errorf("Error reading file: %v", err)
	}
	fingerprint := hex.EncodeToString(h.Sum(nil))

	var junk int
	if err := db.QueryRow("SELECT 1 FROM uploads WHERE fingerprint = ?", fingerprint).Scan(&junk); err == nil {
		return fmt.Errorf("image fingerprint already exists in database")
	}

	upload.Seek(0, 0)
	request, err := http.NewRequest("POST", "https://photoslibrary.googleapis.com/v1/uploads", upload)
	if err != nil {
		return fmt.Errorf("Error creating request: %v", err)
	}
	token.SetAuthHeader(request)
	request.Header.Set("Content-type", "application/octet-stream")
	request.Header.Set("X-Goog-Upload-Protocol", "raw")
	request.Header.Set("X-Goog-Upload-File-Name", filepath.Base(filename))
	resp, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("Error uploading image: %v", err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Error reading upload token: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("Bad stats uploading: %v", err)
	}
	uploadToken := string(body)

	// add to library
	bod := &bytes.Buffer{}
	if err := json.NewEncoder(bod).Encode(map[string]interface{}{
		"newMediaItems": []map[string]interface{}{
			{
				"description": "",
				"simpleMediaItem": map[string]string{
					"uploadToken": uploadToken,
				},
			},
		},
	}); err != nil {
		return fmt.Errorf("Error marshaling newMediaItem: %v", err)
	}
	request, err = http.NewRequest("POST", "https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate", bod)
	if err != nil {
		return fmt.Errorf("Error creating request: %v", err)
	}
	token.SetAuthHeader(request)
	resp, err = client.Do(request)
	if err != nil {
		return fmt.Errorf("Error adding media item: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("Bad status adding media item: %v", resp.StatusCode)
	}

	_, err = db.Exec("INSERT INTO uploads (fingerprint, filename, uploaded) VALUES (?, ?, ?)", fingerprint, filepath.Base(filename), time.Now().Unix())
	return err
}
