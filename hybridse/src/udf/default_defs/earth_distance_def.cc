/**
 * Copyright (c) 2023 4Paradigm Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cmath>

#include "udf/default_udf_library.h"
#include "udf/udf_library.h"
#include "udf/udf_registry.h"

namespace hybridse {
namespace udf {

static constexpr double EarthRadiusKm = 6372.8;

inline double degree_2_radian(double angle) { return M_PI * angle / 180.0; }

class Earth {
 public:
    Earth(double latitude, double longitude) : latitude_(latitude), longitude_(longitude) {}

    double Latitude() const { return latitude_; }

    double Longitude() const { return longitude_; }

 private:
    double latitude_;
    double longitude_;
};

double haversine_distance(const Earth& p1, const Earth& p2) {
    double latRad1 = degree_2_radian(p1.Latitude());
    double latRad2 = degree_2_radian(p2.Latitude());
    double lonRad1 = degree_2_radian(p1.Longitude());
    double lonRad2 = degree_2_radian(p2.Longitude());

    double diffLa = latRad2 - latRad1;
    double doffLo = lonRad2 - lonRad1;

    double computation =
        asin(sqrt(sin(diffLa / 2) * sin(diffLa / 2) + cos(latRad1) * cos(latRad2) * sin(doffLo / 2) * sin(doffLo / 2)));
    return 2 * EarthRadiusKm * computation;
}

void haversine_distance_d4(double ll1, double ll2, double rl1, double rl2, double* output, bool* is_null) {
    *output = haversine_distance(Earth(ll1, ll2), Earth(rl1, rl2));
    *is_null = false;
}

void DefaultUdfLibrary::InitEarthDistanceUdf() {
    std::string doc = R"(
        @brief Returns the great circle distance between two points on the surface of the Earth. Km as return unit.
        add a minus (-) sign if heading west (W) or south (S).

        @param ll1 First latitude in degree
        @param ll2 First longitude in degree
        @param rl1 second latitude in degree
        @param rl2 Second longitude in degree

        Example:

        @code{.sql}
             select earth_distance(40, 73, 41, 74)
             -- output 139.7
        @endcode

        @since 0.8.0)";
    RegisterExternal("earth_distance")
        .args<double, double, double, double>(reinterpret_cast<void*>(haversine_distance_d4))
        .return_by_arg(true)
        .returns<Nullable<double>>()
        .doc(doc);

    RegisterExprUdf("earth_distance")
        .args<AnyArg, AnyArg, AnyArg, AnyArg>(
            [](UdfResolveContext* ctx, ExprNode* ll1, ExprNode* ll2, ExprNode* rl1, ExprNode* rl2) {
                auto nm = ctx->node_manager();
                if (ll1->GetOutputType()->base() != node::kDouble) {
                    ll1 = nm->MakeCastNode(node::kDouble, ll1);
                }
                if (ll2->GetOutputType()->base() != node::kDouble) {
                    ll2 = nm->MakeCastNode(node::kDouble, ll2);
                    if (rl1->GetOutputType()->base() != node::kDouble) {
                        rl1 = nm->MakeCastNode(node::kDouble, rl1);
                    }
                }
                if (rl2->GetOutputType()->base() != node::kDouble) {
                    rl2 = nm->MakeCastNode(node::kDouble, rl2);
                }

                return nm->MakeFuncNode("earth_distance", {ll1, ll2, rl1, rl2}, nullptr);
            })
        .doc(doc);
}
}  // namespace udf
}  // namespace hybridse
