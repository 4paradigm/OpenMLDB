// The MIT License (MIT)
//
// Copyright (c) 2015
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
//     of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
//     to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//     copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
//     copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//     AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include "apiserver/interface_provider.h"

#include <deque>

#include "boost/algorithm/string/split.hpp"
#include "glog/logging.h"

namespace openmldb {
namespace apiserver {

std::vector<std::unique_ptr<PathPart>> Url::parsePath(bool disableIds) const {
    std::deque<std::string> split_res;
    boost::algorithm::split(split_res, path, [](char c) { return c == '/'; });
    split_res.pop_front();

    std::vector<std::unique_ptr<PathPart>> splitPath;
    for (auto const& i : split_res) {
        if (!disableIds && i.front() == ':') {
            splitPath.emplace_back(new PathParameter(i.substr(1, i.length() - 1)));
        } else {
            splitPath.emplace_back(new PathString(i));
        }
    }
    return splitPath;
}

PathParameter::PathParameter(std::string id) : value_(), id_(std::move(id)) {}

std::string PathParameter::getValue() const { return value_; }

std::string PathParameter::getId() const { return id_; }

void PathParameter::setValue(std::string const& value) { value_ = value; }

PathType PathParameter::getType() const { return PathType::PARAMETER; }

PathString::PathString(std::string value) : value_(std::move(value)) {}

std::string PathString::getValue() const { return value_; }

PathType PathString::getType() const { return PathType::STRING; }

void ReducedUrlParser::parseQuery(std::string const& query, Url* url) {
    static const std::regex query_reg{R"((\w+=(?:[\w-])+)(?:(?:&|;)(\w+=(?:[\w-])+))*)"};
    std::smatch match;
    if (std::regex_match(query, match, query_reg)) {
        for (auto i = std::begin(match) + 1; i < std::end(match); ++i) {
            auto pos = i->str().find_first_of('=');
            url->query[i->str().substr(pos + 1)] = i->str().substr(0, pos);
        }
    }
}

bool ReducedUrlParser::parse(std::string const& urlString, Url* url) {
    static const std::regex reg{
        R"((?:(?:(\/(?:(?:[a-zA-Z0-9]|[-_~!$&']|[()]|[*+,;=:@])+(?:\/(?:[a-zA-Z0-9]|[-_~!$&']|[()]|[*+,;=:@])+)*)?)|\/)?(?:(\?(?:\w+=(?:[\w-])+)(?:(?:&|;)(?:\w+=(?:[\w-])+))*))?(?:(#(?:\w|\d|=|\(|\)|\\|\/|:|,|&|\?)+))?))"};

    url->url = urlString;

    // regex for extracting path, query, fragment
    std::smatch match;
    if (!std::regex_match(urlString, match, reg)) {
        return false;
    }
    for (auto i = std::begin(match) + 1; i < std::end(match); ++i) {
        if (i->str().front() == '/') {
            url->path = i->str();
        } else if (i->str().front() == '?') {
            parseQuery(i->str().substr(1, i->str().length() - 1), url);
        } else if (i->str().front() == '#') {
            url->fragment = i->str().substr(1, i->str().length() - 1);
        }
    }

    return true;
}

InterfaceProvider& InterfaceProvider::get(const std::string& path, std::function<func> callback) {
    registerRequest(brpc::HttpMethod::HTTP_METHOD_GET, path, std::move(callback));
    return *this;
}

InterfaceProvider& InterfaceProvider::put(const std::string& path, std::function<func> callback) {
    registerRequest(brpc::HttpMethod::HTTP_METHOD_PUT, path, std::move(callback));
    return *this;
}

InterfaceProvider& InterfaceProvider::post(const std::string& path, std::function<func> callback) {
    registerRequest(brpc::HttpMethod::HTTP_METHOD_POST, path, std::move(callback));
    return *this;
}

bool InterfaceProvider::matching(const Url& received, const Url& registered) {
    auto registeredParts = registered.parsePath();
    auto receivedParts = received.parsePath(true);

    if (registeredParts.size() != receivedParts.size()) {
        return false;
    }

    for (std::size_t i = 0; i != registeredParts.size(); ++i) {
        if (registeredParts[i]->getType() == PathType::STRING) {
            // check if path string parts are equal
            if (registeredParts[i]->getValue() != receivedParts[i]->getValue()) {
                return false;
            }
        }
    }
    return true;
}

std::unordered_map<std::string, std::string> InterfaceProvider::extractParameters(const Url& received,
                                                                                  const Url& registered) {
    auto registeredParts = registered.parsePath();
    auto receivedParts = received.parsePath(true);

    //    assert(registeredParts.size() == receivedParts.size());

    std::unordered_map<std::string, std::string> map;
    for (std::size_t i = 0; i != registeredParts.size(); ++i) {
        if (registeredParts[i]->getType() == PathType::PARAMETER) {
            map[static_cast<PathParameter*>(registeredParts[i].get())->getId()] = receivedParts[i]->getValue();
        }
    }
    return map;
}

void InterfaceProvider::registerRequest(brpc::HttpMethod type, std::string const& url, std::function<func>&& callback) {
    Url parsed;
    if (!ReducedUrlParser::parse(url, &parsed)) {
        LOG(ERROR) << "Fail to parse url " << url;
        return;
    }
    BuiltRequest req{parsed, callback};
    requests_[type].push_back(req);
}

bool InterfaceProvider::handle(const std::string& path, const brpc::HttpMethod& method, const butil::IOBuf& req_body,
                               JsonWriter& writer) {
    auto err = GeneralResp();
    Url url;

    if (!ReducedUrlParser::parse(path, &url)) {
        writer << err.Set("invalid url");
        return false;
    }

    auto requestList = requests_.find(method);

    // is there any request matching the request type?
    if (requestList == std::end(requests_)) {
        if (strncmp(HttpMethod2Str(method), "UNKNOWN", 7) != 0) {
            writer << err.Set("unsupported method");
            return false;
        }

        writer << err.Set("invalid method");
        return false;
    }

    // is there a registered request, that matches the url?
    auto request = std::find_if(std::begin(requestList->second), std::end(requestList->second),
                                [&, this](BuiltRequest const& request) { return matching(url, request.url); });

    if (request == std::end(requestList->second)) {
        writer << err.Set("no match method");
        return false;
    }

    auto params = extractParameters(url, request->url);
    request->callback(params, req_body, writer);
    return true;
}
}  // namespace apiserver
}  // namespace openmldb
