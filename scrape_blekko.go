package main

import (
    "os"
    "net"
    "net/http"
    "log"
    "fmt"
    "strings"
    "regexp"
    "time"
    "sync"
    "encoding/json"
    "code.google.com/p/go.net/html"
    //"github.com/opesun/goquery"
    "code.google.com/p/go-html-transform/html/transform"
    "code.google.com/p/go-html-transform/h5"
)

type ScraperConfig struct {UserAgent string}

type Category struct {
    Name string `json:"name"`
    Urls []string `json:"urls"`
}

type CategoryList struct {
    Data []Category `json:"d"`
}

var (
    blekkoSubCat = regexp.MustCompile("^/blekko/")
    scraperConfig = ScraperConfig {UserAgent: "titleScraper/1.1"}
    categorySet = make(map[string]bool)
    workPool = make(chan bool, 4)
    catLock = sync.Mutex{}
    categoryList = CategoryList{}
)

func timeoutDialler(timeout time.Duration) func(net, addr string) (client net.Conn, err error) {
    return func(netw, addr string) (net.Conn, error) {
        client, err := net.DialTimeout(netw, addr, time.Duration(30*time.Second))
        if err != nil {
            return nil, err
        }
        client.SetDeadline(time.Now().Add(timeout))
        return client, nil
    }
}

func fetchTagUrls(url string) []string {
    req, err := http.NewRequest("GET", url, nil)
    req.Header.Set("User-Agent", scraperConfig.UserAgent)

    httpClient := http.Client{
        Transport: &http.Transport{
            Dial: timeoutDialler(time.Duration(10*time.Second)),
            DisableKeepAlives: true,
        },
    }

    var output = []string{}

    resp, err := httpClient.Do(req)
    if err != nil {
        log.Printf("HTTP_ERROR url:'%s' error:'%s'\n", url, err)
        return output
    }
    defer resp.Body.Close()

    if resp.StatusCode == 200 {
        tree, err := h5.New(resp.Body)
        if err != nil {
            log.Printf("HTML_ERROR url:'%s' error:'%s'\n", url, err)
            return output
        }

        var GetUrls = func(n *html.Node) {
                for _, a := range n.Attr {
                    if a.Key == "href" {
                        output = append(output, a.Val)
                        break
                    }
                }
        }
        t := transform.New(tree)
        t.Apply(GetUrls, "#tags-directory ul li a")
    }
    return output
}

func fetchCategory(url string) Category {
    req, err := http.NewRequest("GET", url, nil)
    req.Header.Set("User-Agent", scraperConfig.UserAgent)

    httpClient := http.Client{
        Transport: &http.Transport{
            Dial: timeoutDialler(time.Duration(10)*time.Second),
            DisableKeepAlives: true,
        },
    }

    var output = Category{}

    resp, err := httpClient.Do(req)
    if err != nil {
        log.Printf("HTTP_ERROR url:'%s' error:'%s'\n", url, err)
        return output
    }
    defer resp.Body.Close()

    if resp.StatusCode == 200 {

        tree, err := h5.New(resp.Body)
        if err != nil {
            log.Printf("HTML_ERROR url:'%s' error:'%s'\n", url, err)
            return output
        }

        pathFragments := strings.Split(url, "/")
        output.Name = pathFragments[len(pathFragments)-1];
        log.Println("Processing", output.Name)

        if !categorySet[output.Name] {
            // prevent cycles. this is wonky, but will do for now
            t := transform.New(tree)
            var getUrls = func(n *html.Node) {
                urls := strings.Split(n.FirstChild.Data, "\n")
                for _, item := range urls {
                    item = strings.TrimSpace(item)
                    // if we encounter a subcategory, recurse
                    if blekkoSubCat.MatchString(item) {
                        subCatUrl := fmt.Sprintf("https://blekko.com/ws/+/view+%s", item)
                        subCat := fetchCategory(subCatUrl)
                        for _, subUrl := range subCat.Urls {
                            output.Urls = append(output.Urls, subUrl)
                        }
                    } else if item != "" {
                        output.Urls = append(output.Urls, item)
                    }
                }
            }
            t.Apply(getUrls, "#urls-text")

            categorySet[output.Name] = true
        }
    }
    return output
}

func fetchCategoryGQ(url string) Category {
    req, err := http.NewRequest("GET", url, nil)
    req.Header.Set("User-Agent", scraperConfig.UserAgent)

    httpClient := http.Client{
        Transport: &http.Transport{
            Dial: timeoutDialler(time.Duration(10*time.Second)),
            DisableKeepAlives: true,
        },
    }

    var output = Category{}

    resp, err := httpClient.Do(req)
    if err != nil {
        log.Printf("HTTP_ERROR url:'%s' error:'%s'\n", url, err)
        return output
    }
    defer resp.Body.Close()

    if resp.StatusCode == 200 {

        tree, err := h5.New(resp.Body)
        if err != nil {
            log.Printf("HTML_ERROR url:'%s' error:'%s'\n", url, err)
            return output
        }

        pathFragments := strings.Split(url, "/")
        output.Name = pathFragments[len(pathFragments)-1];
        log.Println("Processing", output.Name)

        if !categorySet[output.Name] {
            // prevent cycles. this is wonky, but will do for now

            /*
            nodes := doc.Find("#urls-text")
            if len(nodes) == 1 {
            }
            */

            t := transform.New(tree)
            var getUrls = func(n *html.Node) {
                urls := strings.Split(n.FirstChild.Data, "\n")
                for _, item := range urls {
                    item = strings.TrimSpace(item)
                    if blekkoSubCat.MatchString(item) {
                        /*
                        // if we encounter a subcategory, recurse
                        subCat := fetchCategory(subCatUrl)
                        for _, subUrl := range subCat.Urls {
                            output.Urls = append(output.Urls, subUrl)
                        }
                        */
                        // make n-level categories 1st level
                        subCatUrl := fmt.Sprintf("https://blekko.com/ws/+/view+%s", item)
                        go downloadUrls(subCatUrl)
                    } else if item != "" {
                        output.Urls = append(output.Urls, item)
                    }
                }
            }
            t.Apply(getUrls, "#urls-text")

            categorySet[output.Name] = true
        }
    }
    return output
}

func downloadUrls(categoryUrl string) {
    workPool <- true
    var download = func(url string) {
        category := fetchCategory(url)
        if category.Name != "" && category.Urls != nil {
            catLock.Lock()
            categoryList.Data = append(categoryList.Data, category)
            catLock.Unlock()
        }
        <-workPool
    }
    go download(categoryUrl)
}

func main() {
    log.Println("Starting blekko scraper")

    for _, tagUrl := range fetchTagUrls("https://blekko.com/tag/show") {
        categoryUrl := fmt.Sprintf("https://blekko.com%s", tagUrl)
        downloadUrls(categoryUrl)
    }
    data, err := json.MarshalIndent(categoryList, "", "    ")
    if err != nil {
        log.Println("ERROR:", err)
    }
    os.Stdout.Write(data)
    log.Println("scraping job done")
}
