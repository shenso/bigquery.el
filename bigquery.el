;; bigquery.el - BigQuery modes for GNU Emacs -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Shawn Henson

;; Author: Shawn Henson <shawn@shenso.name>
;; Version: 0.1.0
;; Package-Requires: ()
;; Keywords: sql, bigquery, gcloud, google cloud
;; URL: https://github.com/shenso/bigquery.el

;; This program is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; This package provides major modes for working with GoogleSQL and interacting
;; with BigQuery projects, datasets, and tables.

(require 'cl-lib)

(defvar googlesql-reserved-keywords
  '("all" "and" "any" "array" "as" "asc" "assert_rows_modified" "at" "between"
    "by" "case" "cast" "collate" "contains" "create" "cross" "cube" "current"
    "default" "define" "desc" "distinct" "else" "end" "enum" "escape" "except"
    "exclude" "exists" "extract" "false" "fetch" "following" "for" "from" "full"
    "group" "grouping" "groups" "hash" "having" "if" "ignore" "in" "inner"
    "intersect" "interval" "into" "is" "join" "lateral" "left" "like" "limit"
    "lookup" "merge" "natural" "new" "no" "not" "null" "nulls" "of" "on" "or"
    "order" "outer" "over" "partition" "preceding" "proto" "qualify" "range"
    "recursive" "respect" "right" "rollup" "rows" "select" "set" "some" "struct"
    "tablesample" "then" "to" "treat" "true" "unbounded" "union" "unnest"
    "using" "when" "where" "window" "with" "within"
    "insert"))

(defvar googlesql-builtin-functions
  '("abs" "acos" "acosh" "aead.decrypt_bytes" "aead.decrypt_string"
    "aead.encrypt" "any_value" "appends" "approx_count_distinct"
    "approx_quantiles" "approx_top_count" "approx_top_sum" "array_agg"
    "array_concat" "array_concat_agg" "array_length" "array_reverse"
    "array_to_string" "ascii" "asin" "asinh" "atan" "atan2" "atanh" "avg"
    "bag_of_words" "bit_and" "bit_count" "bit_or" "bit_xor" "bool" "byte_length"
    "cast" "cbrt" "ceil" "ceiling" "changes" "char_length" "character_length"
    "chr" "code_points_to_bytes" "code_points_to_string" "collate" "concat"
    "contains_substr" "corr" "cos" "cosh" "cosine_distance" "cot" "coth" "count"
    "countif" "covar_pop" "covar_samp" "csc" "csch" "cume_dist" "current_date"
    "current_datetime" "current_time" "current_timestamp" "date" "date_add"
    "date_bucket" "date_diff" "date_from_unix_date" "date_sub" "date_trunc"
    "datetime" "datetime_add" "datetime_bucket" "datetime_diff" "datetime_sub"
    "datetime_trunc" "dense_rank" "deterministic_decrypt_bytes"
    "deterministic_decrypt_string" "deterministic_encrypt" "div"
    "dlp_deterministic_encrypt" "dlp_deterministic_decrypt" "dlp_key_chain"
    "float64" "edit_distance" "ends_with" "error" "exp"
    "external_object_transform" "external_query" "extract" "euclidean_distance"
    "farm_fingerprint" "first_value" "floor" "format_date" "format_datetime"
    "format_time" "format_timestamp" "format" "from_base32" "from_base64"
    "from_hex" "gap_fill" "generate_array" "generate_date_array"
    "generate_range_array" "generate_timestamp_array" "generate_uuid" "greatest"
    "grouping" "hll_count.extract" "hll_count.init" "hll_count.merge"
    "hll_count.merge_partial" "ieee_divide" "initcap" "instr" "int64" "is_inf"
    "is_nan" "json_array" "json_array_append" "json_array_insert" "json_extract"
    "json_extract_array" "json_extract_scalar" "json_extract_string_array"
    "json_keys" "json_object" "json_query" "json_query_array" "justify_days"
    "justify_hours" "justify_interval" "keys.add_key_from_raw_bytes"
    "keys.keyset_chain" "keys.keyset_from_json" "keys.keyset_length"
    "keys.keyset_to_json" "keys.new_keyset" "keys.new_wrapped_keyset"
    "keys.rewrap_keyset" "keys.rotate_keyset" "keys.rotate_wrapped_keyset"
    "kll_quantiles.extract_int64" "kll_quantiles.extract_float64"
    "kll_quantiles.extract_point_int64" "kll_quantiles.extract_point_float64"
    "kll_quantiles.init_int64" "kll_quantiles.init_float64"
    "kll_quantiles.merge_int64" "kll_quantiles.merge_float64"
    "kll_quantiles.merge_partial" "kll_quantiles.merge_point_int64"
    "kll_quantiles.merge_point_float64" "lag" "last_day" "last_value" "lax_bool"
    "lax_float64" "lax_int64" "lax_string" "lead" "least" "length" "ln" "log"
    "log10" "logical_and" "logical_or" "lower" "lpad" "ltrim" "make_interval"
    "max" "max_by" "md5" "min" "min_by" "mod" "net.host" "net.ip_from_string"
    "net.ip_net_mask" "net.ip_to_string" "net.ip_trunc" "net.ipv4_from_int64"
    "net.ipv4_to_int64" "net.public_suffix" "net.reg_domain"
    "net.safe_ip_from_string" "normalize" "normalize_and_casefold" "nth_value"
    "ntile" "obj.fetch_metadata" "obj.get_access_url" "obj.make_ref"
    "octet_length" "parse_bignumeric" "parse_date" "parse_datetime" "parse_json"
    "parse_numeric" "parse_time" "parse_timestamp" "percent_rank"
    "percentile_cont" "percentile_disc" "pow" "power" "rand" "range"
    "range_bucket" "range_contains" "range_end" "range_intersects"
    "range_overlaps" "range_sessionize" "range_start" "rank" "regexp_contains"
    "regexp_extract" "regexp_extract_all" "regexp_instr" "regexp_replace"
    "regexp_substr" "repeat" "replace" "reverse" "right" "round" "row_number"
    "rpad" "rtrim" "s2_cellidfrompoint" "s2_coveringcellids" "safe_add"
    "safe_cast" "safe_convert_bytes_to_string" "save_divide" "safe_multiply"
    "safe_negate" "safe_subtract" "search" "sec" "sech" "session_user" "sha1"
    "sha256" "sha512" "sign" "sin" "sinh" "soundex" "split" "sqrt" "st_angle"
    "st_area" "st_asbinary" "st_asgeojson" "st_astext" "st_azimuth"
    "st_boundary" "st_boundingbox" "st_buffer" "st_bufferwithtolerance"
    "st_centroid" "st_centroid_agg" "st_closestpoint" "st_clusterdbscan"
    "st_contains" "st_convexhull" "st_coveredby" "st_covers" "st_difference"
    "st_dimension" "st_disjoint" "st_distance" "st_dump" "st_dwithin"
    "st_endpoint" "st_equals" "st_extent" "st_exteriorring" "st_geogfrom"
    "st_geogfromgeojson" "st_geogfromtext" "st_geogfromwkb" "st_geogpoint"
    "st_geogpointfromgeohash" "st_geohash" "st_geometrytype"
    "st_hausdorffdistance" "st_hausdorffdwithin" "st_interiorrings"
    "st_intersection" "st_intersects" "st_intersectbox" "st_isclosed"
    "st_iscollection" "st_iscollection" "st_isempty" "st_isring" "st_length"
    "st_lineinterpolatepoint" "st_linelocatepoint" "st_linesubstring"
    "st_makeline" "st_makepolygon" "st_makepolygonoriented" "st_maxdistance"
    "st_npoints" "st_numgeometries" "st_numpoints" "st_perimeter" "st_pointn"
    "st_regionstats" "st_simplify" "st_snaptogrid" "st_startpoint" "st_touches"
    "st_union" "st_union_agg" "st_within" "st_x" "st_y" "starts_with" "stddev"
    "stddev_pop" "stddev_samp" "string" "string_agg" "strpos" "substr"
    "substring" "sum" "tan" "tanh" "text_analyze" "tf_idf" "time" "time_add"
    "time_diff" "time_sub" "time_trunc" "timestamp" "timestamp_add"
    "timestamp_bucket" "timestamp_diff" "timestamp_micros" "timestamp_millis"
    "timestamp_seconds" "timestamp_sub" "timestamp_trunc" "to_base32"
    "to_base64" "to_code_points" "to_hex" "to_json" "to_json_string" "translate"
    "trim" "trunc" "typeof" "unicode" "unix_date" "unix_micros" "unix_millis"
    "unix_seconds" "upper" "var_pop" "var_samp" "variance" "vector_search"
    ;; neither a reserved keyword, nor function, but fits as the latter
    "coalesce"
    ))

(defconst googlesql-jinja-keywords
  '("block" "endblock" "for" "endfor" "if" "elif" "else" "endif" "macro"
    "endmacro" "call" "endcall" "filter" "endfilter" "set" "endset" "include"
    "extends"
    "and" "or" "not"
    "in" "is"))

(defconst googlesql-jinja-builtins
  '(
    ;; filters
    "abs" "attr" "batch" "capitalize" "center" "default" "dictsort" "escape"
    "filesizeformat" "first" "float" "forceescape" "format" "groupby" "indent"
    "int" "items" "join" "last" "length" "list" "lower" "map" "max" "min"
    "pprint" "random" "reject" "rejectattr" "replace" "reverse" "round" "safe"
    "select" "selectattr" "slice" "sort" "string" "striptags" "sum" "title"
    "tojson" "trim" "truncate" "unique" "upper" "urlencode" "urlize" "wordcount"
    "wordwrap" "xmlattr"
    ;; tests
    "boolean" "callable" "defined" "divisibleby" "eq" "escaped" "even" "false"
    "filter" "float" "ge" "gt" "in" "integer" "iterable" "le" "lower" "lt"
    "mapping" "ne" "none" "number" "odd" "sameas" "sequence" "string" "test"
    "true" "undefined" "upper"
    ;; global functions
    "range" "lipsum" "dict" "cycler" "joiner" "namespace"
    ;; dbt functions
    "ref" "source" "config"))

(defconst googlesql-jinja-constants
  '("True" "False" "None"))

(defconst googlesql-construct-symbols
  '("with" "select" "create" "alter" "drop" "undrop" "insert" "delete" "truncate"
    "update" "merge" "grant" "revoke" "declare" "set" "execute" "begin"
    "exception" "end" "if" "loop" "repeat" "while" "break" "leave" "continue"
    "iterate" "for" "commit" "rollback" "raise" "return" "call" "export" "load"
    "assert" "union"))

(defconst googlesql-construct-clause-symbols
  '("with" "from" "where" "group" "having" "qualify" "window" "on" "order"
    "limit" "partition" "over"))

(defconst googlesql-conjunction-symbols
  '("and" "or"))

(defconst googlesql-expr-block-predecessor-tokens
  '("with" "select" "from" "where" "having" "qualify" "window" "on" "as"
    "over" "and" "or" "not" "array"))

(defconst googlesql-clause-symbol-alist
  '((with      . differential-privacy-clause)
    (from      . from-clause)
    (where     . where-clause)
    (group     . group-by-clause)
    (having    . having-clause)
    (qualify   . qualify-clause)
    (window    . window-clause)
    (on        . on-clause)
    (using     . using-clause)
    (order     . order-by-clause)
    (partition . partition-by-clause)
    (limit     . limit-clause)
    (over      . over-clause)))

(defun googlesql-symbol-regexp-opt (symbol-list)
  (rx-to-string `(seq symbol-start
                      ,(cons 'or symbol-list)
                      symbol-end)))

(defconst googlesql-construct-clause-regexp
  (googlesql-symbol-regexp-opt googlesql-construct-clause-symbols))

(defconst googlesql-syntactic-symbol-regexp
  (googlesql-symbol-regexp-opt (seq-union googlesql-construct-symbols
                                          googlesql-construct-clause-symbols)))

(defconst googlesql-conjunction-symbol-regexp
  (googlesql-symbol-regexp-opt googlesql-conjunction-symbols))

(defconst googlesql-expr-block-predecessor-token-regexp
  (googlesql-symbol-regexp-opt googlesql-expr-block-predecessor-tokens))

(defun googlesql--font-lock-matcher (symbol-regexp &optional jinja)
  (lambda (limit)
    (let ((case-fold-search (not jinja)))
      (when (re-search-forward symbol-regexp limit 'move)
        (if (googlesql-syntax-context 'jinja) jinja (not jinja))))))

(defun googlesql-build-font-lock-keyword (keywords face &optional jinja)
  (cons (googlesql--font-lock-matcher (googlesql-symbol-regexp-opt keywords)
                                      jinja)
        face))

(defvar googlesql-font-lock-keywords
  `(,(googlesql-build-font-lock-keyword googlesql-builtin-functions
                                        'font-lock-builtin-face)
    ,(googlesql-build-font-lock-keyword googlesql-reserved-keywords
                                        'font-lock-keyword-face)
    ,(googlesql-build-font-lock-keyword googlesql-jinja-builtins
                                        'font-lock-builtin-face
                                        t)
    ,(googlesql-build-font-lock-keyword googlesql-jinja-keywords
                                        'font-lock-keyword-face
                                        t)
    ,(googlesql-build-font-lock-keyword googlesql-jinja-constants
                                        'font-lock-constant-face
                                        t)))

(defcustom googlesql-basic-offset 4
  "The offset used in indentation")

(defcustom googlesql-offsets-alist
  `((topmost-intro               . 0)
    (string                      . 0)
    (comment-continuation        . 1)
    (arglist-open                . +)
    (arglist-intro               . +)
    (arglist-continuation        . 0)
    (arglist-expr-continuation   . +)
    (arglist-close               . 0)
    (expr-block-open             . +)
    (expr-block-intro            . +)
    (expr-block-continuation     . 0)
    (expr-block-close            . 0)
    (statement-construct         . 0)
    (select-continuation         . +)
    (select-list                 . +)
    (select-list-continuation    . +)
    (field-alias                 . +)
    (differential-privacy-clause . +)
    (from-clause                 . 0)
    (where-clause                . 0)
    (group-by-clause             . 0)
    (having-clause               . 0)
    (qualify-clause              . 0)
    (window-clause               . 0)
    (on-clause                   . +)
    (using-clause                . +)
    (order-by-clause             . 0)
    (partition-by-clause         . 0)
    (limit-clause                . 0)
    (over-clause                 . +)
    (cte-item                    . +)
    (from-item                   . +)
    (window-spec-intro           . +)
    (window-spec-item            . +)
    (join-operation              . 0)
    (join-continuation           . 0)
    (join-continuation           . 0)
    (clause-bool-expression      . +)
    (clause-list-item            . +)
    (expression-continuation     . +)
    )
  "Offset rules used in indentation")

(defvar googlesql-syntax-table
  (let ((table (make-syntax-table)))
    ;; C style comments
    (modify-syntax-entry ?/ ". 14" table)
    (modify-syntax-entry ?* ". 23" table)
    ;; double-dash comments
    (modify-syntax-entry ?- ". 12b" table)
    ;; pound style comments
    (modify-syntax-entry ?# "< b" table)
    ;; newline end comments
    (modify-syntax-entry ?\n  "> b" table)
    (modify-syntax-entry ?\f  "> b" table)
    (modify-syntax-entry ?\^m "> b" table)
    ;; strings
    (modify-syntax-entry ?\" "'" table)
    (modify-syntax-entry ?'  "'" table)
    (modify-syntax-entry ?`  "'" table) ; quoted identifiers
    ;; symbols -> punctuation
    (modify-syntax-entry ?&         "." table)
    (modify-syntax-entry ?+         "." table)
    (modify-syntax-entry '(?< . ?>) "." table)
    (modify-syntax-entry ?|         "." table)
    ;; punctuation -> symbols
;;    (modify-syntax-entry ?. "_" table)
    table))

(defvar-keymap googlesql-mode-map
    "C-c C-s" #'googlesql-show-syntactic-information)

(defmacro bigquery-safe (expr err-expr)
  `(condition-case nil
       ,expr
     (error ,err-expr)))

(defun googlesql-level-start (ch prev-level-start)
  (if prev-level-start
      (let (level-start)
        (while (and (setq level-start (car prev-level-start))
                    (not (eq (char-after level-start) ch)))
          (setq prev-level-start (delq level-start prev-level-start)))
        (cons level-start prev-level-start))
    nil))

(defsubst googlesql-level-start-ppss (ch ppss &optional reverse)
  (googlesql-level-start ch (if reverse (reverse (nth 9 ppss)) (nth 9 ppss))))

(defun googlesql-syntax-context (type &optional ppss)
  (unless ppss (setq ppss (syntax-ppss)))

  (pcase type
    ('comment
     (and (nth 4 ppss) (nth 8 ppss)))
    ('string
     (and (nth 3 ppss) (nth 8 ppss)))
    ('paren
     (car (googlesql-level-start-ppss ?\( ppss t)))
    ('bracket
     (car (googlesql-level-start-ppss ?\[ ppss t)))
    ('jinja
     (let ((brace (googlesql-level-start-ppss ?{ ppss nil))
           needle)
       (while (and brace (null needle))
         (when (memq (char-after (car brace)) '(?{ ?%))
           (setq needle (car brace)))
         (setq brace (googlesql-level-start ?{ (cdr brace))))
       needle))
    (_ nil)))

(defun googlesql-skip-comment (&optional direction)
  (let ((comment-start (googlesql-syntax-context 'comment))
        (factor (if (< (or direction 0) 0)
                    -99999
                  9999)))
    (when comment-start
      (goto-char comment-start))
    (forward-comment factor)))

(defmacro googlesql-point (type &optional point)
  `(save-excursion
     ,@(if point `((goto-char ,point)))
     ,@(pcase (eval type)
         ('boi      `((beginning-of-line)           ; beginning of indent
                      (skip-chars-forward " \t")))
         ('fw       `((googlesql-skip-comment 1)))  ; forward whitespace
         ('bw       `((googlesql-skip-comment -1))) ; forward whitespace
         ('backsexp `((backward-sexp) (point))))    ; backward s-expression
     (point)))


(defun googlesql-region-delimited (start end &optional delimiters backward context-paren check-paren)
  "Returns the first delimiter found if any. If `backward' is nil, search from
start to end, otherwise end to start.

If `check-paren' is non-nil, then only characters within contained within the
paren at position `context-paren' are checked. If `context-paren' is non-nil
then `check-paren' defaults to t."
  (setq delimiters  (or delimiters '(?\;))
        check-paren (or context-paren check-paren))

  (let (found)
    (save-excursion
      (goto-char (if backward end start))
      
      (while (and (if backward (> (point) start) (< (point) end))
                  (or (not check-paren)
                      (eq context-paren (googlesql-syntax-context 'paren)))
                  (not (setq found (and (memq (char-after) delimiters)
                                        (null (googlesql-syntax-context 'comment))))))
        (if backward
            (backward-char)
          (forward-char)))
      (when found
        (point)))))

(defmacro googlesql-delimiter-reached (prev-pos &optional delimiters context-paren check-paren)
  `(let* ((cur-pos (point))
          (forward (> cur-pos ,prev-pos)))
     (googlesql-region-delimited (if forward ,prev-pos cur-pos)
                                 (if forward cur-pos ,prev-pos)
                                 ,delimiters
                                 (not forward)
                                 ,context-paren
                                 ,check-paren)))

(defmacro googlesql-boc-save-error-info (missing got)
  `(setq saved-pos (vector pos ,missing ,got)))

(defmacro googlesql-boc-report-error ()
  '(unless noerror
     (setq googlesql-parsing-error
           (format-message
            "No matching `%s' found for `%s' on line %d"
            (elt saved-pos 1)
            (elt saved-pos 2)
            (1+ (count-lines (point-min) (elt saved-pos 0)))))))

(defmacro googlesql-boc-push-state ()
  `(setq stack (cons (cons state saved-pos)
                     stack)))

(defmacro googlesql-boc-pop-state (&rest on-done)
  `(if (setq state     (car (car stack))
             saved-pos (cdr (car stack))
             stack     (cdr stack))
       t
     ,@on-done
     (throw 'loop nil)))

(defmacro googlesql-boc-in-statement-p ()
  `(and statement (bigquery-safe (scan-sexps pos -1) nil)))

(defmacro googlesql-boc-pop-unless-statement (new-state)
  `(if (googlesql-boc-in-statement-p) 
       (setq state ,new-state)
     (googlesql-boc-pop-state)))

(defun googlesql-beginning-of-construct (&optional statement limit noerror)
  "Moves point to the beginning of the query expression or containing statement,
whichever comes first (or is applicable). If statement is non-nil, then point is
always moved to the start of the statement if it is contained within the current
S expression.

Returns a cons cell of the form (SYMBOL . POS)"
  (unless limit
    (setq limit (point-min)))

  (let ((pos   (point))
        (start (point))
        stack
        state
        saved-pos
        sym
        realsym ; last read symbol that is not a delimiter or similar token
        tok
        ;; unfortunate temp variable to determine if the union state handler has
        ;; encountered any symbol other than select, delimiter, or lparen
        union-dirty)
    (if (googlesql-syntax-context 'comment)
        (googlesql-skip-comment -1)
      (skip-chars-forward " \t"))
    (setq pos (point))

    (while
        (and
         (>= pos limit)
         (catch 'loop
           (when (and (null sym) (null (googlesql-syntax-context 'comment)))
             (cond ((looking-at googlesql-syntactic-symbol-regexp)
                    (setq sym (intern (downcase (match-string 0)))
                          realsym sym
                          tok pos))
                   ((eq (char-after) "(")
                    (setq sym 'lparen
                          tok pos))))

           (or
            ;; state handler
            (cond
             ((eq state 'select)
              (cond ((eq sym 'as)
                     (setq state 'as-query-expr)
                     (googlesql-boc-save-error-info 'create 'as))
                    ((memq sym '(delimiter insert))
                     (googlesql-boc-pop-state))
                    ((eq sym 'union)
                     (setq state 'union)
                     (googlesql-boc-save-error-info 'select 'union))
                    (t)))

             ((eq state 'select-clause)
              (cond ((eq sym 'select)
                     (googlesql-boc-pop-unless-statement 'select))
                    ((eq sym 'delimiter)
                     (googlesql-boc-report-error)
                     (googlesql-boc-pop-state))
                    (t)))

             ((eq state 'with)
              (if (memq sym '(select delimiter))
                  (googlesql-boc-pop-state)
                (googlesql-boc-report-error)
                (googlesql-boc-pop-state (setq pos saved-pos))))

             ((eq state 'union)
              (cond ((eq sym 'select)
                     (setq union-dirty nil)
                     (googlesql-boc-pop-unless-statement 'select))
                    ((eq sym 'lparen)
                     (if union-dirty
                         (progn
                           (googlesql-boc-report-error)
                           (googlesql-boc-pop-state))
                       (setq union-dirty nil)
                       (googlesql-boc-pop-unless-statement 'select)))
                    ((eq sym 'delimiter)
                     (googlesql-boc-report-error)
                     (googlesql-boc-pop-state))
                    (t
                     (setq union-dirty t)))))

            ;; nil state fallback
            (cond
             ((eq sym 'delimiter)
              (googlesql-boc-pop-state))

             ((memq sym '(from where group having qualify window))
              (googlesql-boc-push-state)
              (googlesql-boc-save-error-info 'select sym)
              (setq state 'select-clause))

             ((eq sym 'select)
              (unless (googlesql-boc-in-statement-p)
                (throw 'loop nil))
              (googlesql-boc-push-state)
              (googlesql-boc-save-error-info '(as insert with) 'select)
              (setq state 'select))

             ((eq sym 'with)
              ;; we're done if there's no previous symbols
              (unless (bigquery-safe (scan-sexps pos -1) nil)
                (throw 'loop nil))
              (setq saved-pos pos)
              (googlesql-boc-push-state)
              (googlesql-boc-save-error-info 'select 'with)
              (setq state 'with))

             ((eq sym 'union)
              ;; we're done if not looking for a statement. we need to continue
              ;; if not in a statement if just to report errors
              (unless statement
                (throw 'loop nil))
              (googlesql-boc-push-state)
              (googlesql-boc-save-error-info 'select 'union)
              (setq state 'union))))

           ;; move point and check for statement delimiters
           (let ((delimiter-scan-end-pos (point))
                 delimiter-pos)
             (unless (> (point) limit)
               (throw 'loop nil))
             (bigquery-safe (backward-sexp) (throw 'loop nil))
             
             (setq delimiter-pos (googlesql-delimiter-reached delimiter-scan-end-pos))
             (unless (and (>= (point) limit)
                          (or (null delimiter-pos) (>= delimiter-pos limit)))
               (throw 'loop nil))
             (when delimiter-pos
               (setq tok delimiter-pos
                     sym 'delimiter)
               (throw 'loop t)))

           (setq sym nil
                 pos (point)))))

    (when (> (length stack) 0)
      (when (= pos limit)
        (setq pos (or tok pos)))
      ;; TODO: error reporting
      )

    `(,realsym . ,(goto-char pos))))

(defun googlesql-search-backward-sexp (regexp limit &optional delimiters skip)
  "Move backwards in the S expression up to `limit' until the text after point
 matches `regexp'. If a match is found, match-data is updated and the position
 of point is returned, otherwise nil is returned.

If `delimiters' is non-nil, do not search beyond and found delimiter."
  (save-excursion
    (let (pos stopped)
      (when skip
        (dotimes (_ skip)
          (bigquery-safe (backward-sexp) (setq stopped t))))

      (while (and (> (point) limit)
                  (null pos)
                  (not stopped))
        ;; are we looking at the pattern already?
        (if (looking-at regexp)
            (setq pos (point))
          ;; otherwise, move backwards and check for delimiters
          (let ((delimiter-scan-end-pos (point)))
            (bigquery-safe (backward-sexp) (setq stopped t))
            (unless (or stopped (null delimiters))
              (setq stopped (googlesql-delimiter-reached delimiter-scan-end-pos
                                                         delimiters))))))
      pos)))


(defun googlesql--make-symbol-search-lookup-table (seqs &optional require-entire-seq)
  ;; this could be better but our use-case should be simplistic enough for this
  ;; to be good enough
  (let ((sym-table (make-hash-table :test 'equal))
        (seq-count (length seqs)))
    (dolist (seq seqs)
      (when (stringp seq)
        (setq seq (string-split seq " ")))
      (let ((sym-count (length seq)))
        (dotimes (i sym-count)
          (let* ((sym (nth i seq))
                 (entry (gethash sym sym-table nil)))
            (puthash sym
                     (cons
                      (if (> i 0) (cons (nth (1- i) seq) (car entry)) (car entry))
                      (if (< (1+ i) seq-count) (cons (nth (1+ i) seq) (cdr entry)) (cdr entry)))
                     sym-table)))))
    sym-table))

(cl-defun googlesql--looking-at-any-part-of-symbol-sequence (seqs-or-table
                                                             &optional
                                                             require-entire-seq
                                                             (anywhere t)
                                                             no-ignore-case
                                                             &key
                                                             (delimiters '(?\; ?,))
                                                             (context-paren (googlesql-syntax-context 'paren)))
  (let ((case-fold-search (not no-ignore-case))
        (start-pos (point))
        sym-table at-point before after)
    (if (hash-table-p seqs-or-table)
        (setq sym-table seqs-or-table)
      ;; populate the symbol table
      (setq sym-table (googlesql--make-symbol-search-lookup-table seqs-or-table require-entire-seq)))

    (save-excursion
      (when anywhere
        (bigquery-safe
         (beginning-of-thing 'symbol)
         nil))
      (let (start-list cur-list start-found end-found matched-sym has-sentinel
                       prev-pos last-pos)
        (maphash (lambda (k v) (push k start-list)) sym-table)
        (setq cur-list start-list)

        (save-excursion
          (while cur-list
            (setq start-found nil
                  sentinel (memq nil cur-list))
            (setq cur-list (delq nil cur-list))
            (cond ((and cur-list
                        (looking-at (rx-to-string
                                     `(seq symbol-start
                                           (or ,@cur-list)
                                           symbol-end)))
                        (or (null prev-pos)
                            (not (googlesql-delimiter-reached prev-pos delimiters context-paren t))))
                   (setq start-found (point)
                         matched-sym (downcase (match-string 0)))
                   (if at-point
                       (setq before (cons matched-sym before))
                     (setq at-point matched-sym))
                   (setq prev-pos (point))
                   (bigquery-safe (backward-sexp) nil)
                   (setq cur-list (car (gethash matched-sym sym-table nil))))
                  (sentinel
                   (setq start-found prev-pos
                         cur-list nil))
                  (t
                   (setq cur-list nil)))))

        (setq start-list nil)
        (maphash (lambda (k v) (push k start-list)) sym-table)
        (setq cur-list (and start-found start-list))
        (save-excursion
          (while cur-list
            (setq end-found nil
                  sentinel (memq nil cur-list))
            (setq cur-list (delq nil cur-list))
            (cond ((and cur-list
                        (looking-at (rx-to-string
                                     `(seq symbol-start
                                           (or ,@cur-list)
                                           symbol-end)))
                        (or (null last-pos)
                            (not (googlesql-delimiter-reached last-pos delimiters context-paren t))))
                   (setq matched-sym (downcase (match-string 0)))
                   (setq after (cons matched-sym after))
                   (setq last-pos (point))
                   (bigquery-safe (progn
                                    (forward-sexp)
                                    (googlesql-skip-comment 1))
                                  nil)
                   (setq cur-list (cdr (gethash matched-sym sym-table nil))))
                  (sentinel
                   (setq end-found last-pos
                         cur-list nil))
                  (t
                   (setq cur-list nil)))))

        ;; TODO: handle req-entire-seq
        (when start-found
          ;; TODO: validate
          `((,start-pos . ,at-point)
            (,start-found . ,(append before `(,at-point)))
            (,end-found ,last-pos ,after)))))))

(cl-defmacro googlesql--looking-at-fn (name seqs-or-table &optional require-entire-seq)
  `(cl-defun ,name (&key (context-paren nil context-paren-given) (anywhere t))
     (googlesql--looking-at-any-part-of-symbol-sequence ,seqs-or-table
                                                        ,require-entire-seq
                                                        anywhere
                                                        `,@(if context-paren-given `(:context-paren ,context-paren)))))

(defmacro googlesql--match-start-sym (match)
    `(cadr (nth 1 ,match)))

(defmacro googlesql--match-start-tok (match)
  `(and (googlesql--match-start-sym ,match) (car (nth 1 ,match))))

(defmacro googlesql--match-start-intern (match)
  `(bigquery-safe (intern (downcase (googlesql--match-start-sym ,match))) nil))

(defmacro googlesql--match-start-tok (match)
  `(and (googlesql--match-start-sym ,match) (car (nth 1 ,match))))

(defmacro googlesql--match-end-tok (match)
  `(and (googlesql--match-start-sym ,match) (car (nth 2 ,match))))

(googlesql--looking-at-fn googlesql-looking-at-join
                          '("join" "inner join" "full join" "left join"
                            "right join" "cross join" "full outer join"
                            "left outer join" "right outer join"))

(cl-defun googlesql--forward-to (looking-at-fn
                                 &optional
                                 (n 1)
                                 &key
                                 (limit (if (>= n 0) (point-max) (point-min)))
                                 (delimiters '(\;)))
  (let* ((direction (if (= n 0)
                        1
                      (/ n (abs n))))
         (forward (>= direction 0))
         (delimiter-scan-pos (point))
         (count 0)
         thing-info
         prev-thing-info
         delimiter-reached
         at-sexp-boundary)
    (while (and (< count (abs n))
                (or (= count 0) thing-info))
      (setq count (1+ count)
            prev-thing-info thing-info)

      (save-excursion
        (forward-sexp direction)
        (while (and (if forward (< (point) limit) (> (point) limit))
                    (not at-sexp-boundary)
                    (not (setq delimiter-reached (googlesql-delimiter-reached delimiter-scan-pos delimiters)))
                    (null (setq thing-info (funcall looking-at-fn))))
          (setq delimiter-scan-pos (point))
          (condition-case nil
              (forward-sexp direction)
            (scan-error (if (not (memq (save-excursion
                                         (googlesql-skip-comment direction)
                                         (if forward (char-after) (char-before)))
                                       '(?{ ?})))
                            (setq at-sexp-boundary t)
                          (googlesql-skip-comment direction)
                          (forward-char direction))))))

      (when thing-info
        (if forward
            (progn
              (goto-char (nth 1 (nth 2 thing-info)))
              (forward-sexp))
          (goto-char (car (nth 1 thing-info))))))
    thing-info))

(cl-defmacro googlesql--forward-to-fn (name
                                       looking-at-fn
                                       &optional (delimiters nil delimiters-given))
  `(cl-defun ,name (&optional (n 1 n-given)
                              (limit nil limit-given))
     (interactive "p")
     (apply #'googlesql--forward-to ,looking-at-fn
            `(,@(if `,n-given `(,n))
              ,@(if `,limit-given `(:limit ,limit))
              ,@(if ,delimiters-given (list ':delimiters ,delimiters))))))

(cl-defmacro googlesql--backward-to-fn (name forward-to-fn)
  `(cl-defun ,name (&optional (n 1)
                              (limit nil limit-given))
     (interactive "p")
     (apply ,forward-to-fn (- n) `(,@(if `,limit-given `(,limit))))))

(googlesql--forward-to-fn googlesql-forward-join #'googlesql-looking-at-join)
(googlesql--backward-to-fn googlesql-backward-join #'googlesql-forward-join)

(googlesql--forward-to-fn googlesql-forward-clause #'googlesql-looking-at-clause)
(googlesql--backward-to-fn googlesql-backward-clause #'googlesql-forward-clause)

(defun googlesql-backward-join (&optional n limit)
  (interactive)
  (unless n
    (setq n 1))
  (googlesql-forward-join (- n) limit))


(cl-defun googlesql-looking-at-clause (&key (context-paren nil context-paren-given)
                                            (anywhere t))
  "Returns a cons cell of form (CLAUSE . START-POS) when looking at any part of
a clause"
  (let* ((case-fold-search t)
         (clause-sym-seqs '("with differential_privacy options" "from" "where"
                            "group by" "having" "qualify" "window" "on"
                            "order by" "limit" "partition by" "over"))
         (sym-match (apply #'googlesql--looking-at-any-part-of-symbol-sequence
                           clause-sym-seqs nil anywhere nil
                           `(,@(if context-paren-given `(:context-paren ,context-paren))))))
    (cond ((null sym-match) nil)
          ((equal (cadr (nth 1 sym-match)) "with")
           (save-excursion
             (goto-char (car (nth 1 sym-match)))
             (bigquery-safe
              (progn
                (backward-sexp)
                (when (or (looking-at-p (rx symbol-start "select" symbol-end)))
                  sym-match))
              nil)))
          (t
           sym-match))))

(cl-defun googlesql-forward-list-item (&optional
                                       (n 1)
                                       (start-limit (point-min))
                                       (end-limit (point-max))
                                       &key
                                       (context-paren (googlesql-syntax-context 'paren))
                                       (context-boc (save-excursion (googlesql-beginning-of-construct nil start-limit)))
                                       (context-clause (save-excursion (googlesql-backward-clause 1 (or (cdr context-boc) start-limit))))
                                       (context-select-clause nil s-context-select-clause))
  (interactive "p")

  (unless (or (null context-clause) s-context-select-clause)
    (let ((clause-search-limit (min (or (cdr context-boc) start-limit) start-limit)))
      (save-excursion
        (setq context-select-clause context-clause)
        
        ;; ignore clauses that are valid within or above a select list
        (while (and context-select-clause
                    (member (googlesql--match-start-sym context-select-clause) '("over" "with"))
                    (setq context-select-clause (googlesql-backward-clause 1 clause-search-limit)))
          (setq context-select-clause (googlesql-backward-clause 1 clause-search-limit))))))

  (let (result paren-is-boundary)
    (dotimes (i (abs n))
      (when (or (and context-boc (eq (car context-boc) 'select) (null context-clause)) ; inside a select statement
                (member (googlesql--match-start-sym context-clause) '("partition" "group" "order"))
                (setq paren-is-boundary (googlesql-inside-arglist-p start-limit))
                (setq paren-is-boundary (googlesql-inside-struct-p)))
             (let* ((forward (>= n 0))
                    (direction (if forward 1 -1))
                    (start (point))
                    (to-other-item (save-excursion
                                     (googlesql-skip-comment direction)
                                     (eq (if forward (char-after) (char-before))
                                         ?\,)))
                    (to-sym-boundary (bigquery-safe
                                      (and (not to-other-item)
                                           (/= (point)
                                               (if forward
                                                   (end-of-thing 'symbol)
                                                 (beginning-of-thing 'symbol))))
                                      nil))
                    (case-fold-search t)
                    
                    scan-delim-pos found at-clause scan-err moved)
               (save-excursion
                 (googlesql-skip-comment direction)
                 (when to-other-item
                   ;; when at the beginning of an item already, goto the beginning of the
                   ;; next/previous item
                   (forward-char (* 2 direction)))

                 (while (and (if forward
                                 (< (point) end-limit)
                               (and (> (point) start-limit) (> (point) (cdr context-boc))))
                             (not found)
                             (not at-clause)
                             (not scan-err))
                   (setq scan-delim-pos (point))
                   (condition-case nil
                       (progn
                         (forward-sexp direction)
                         (setq found (googlesql-delimiter-reached scan-delim-pos
                                                                  '(?,)
                                                                  context-paren
                                                                  t))
                         (setq at-clause (googlesql-looking-at-clause))
                         (unless at-clause
                           (setq moved t)))
                     (scan-error (setq scan-err t)))))

               (setq result
                     (cond (found
                            (goto-char found)
                            (forward-char)
                            (googlesql-skip-comment (- direction))
                            (point))

                           ((and at-clause (or to-other-item to-sym-boundary moved))
                            (if forward
                                (goto-char (googlesql--match-start-tok at-clause))
                              (goto-char (googlesql--match-end-tok at-clause))
                              (forward-sexp))
                            (googlesql-skip-comment (- direction))
                            (point))

                           ((and context-boc (not forward) (or to-other-item to-sym-boundary))
                            (goto-char (cdr context-boc))
                            (while (or (looking-at-p (rx symbol-start "select" symbol-end))
                                       (googlesql-looking-at-select-continuation)
                                       (googlesql-looking-at-differential-privacy-clause))
                              (forward-sexp 1))
                            (googlesql-skip-comment 1)
                            (point))

                           ((and scan-err paren-is-boundary (or to-other-item to-sym-boundary))
                            (if forward (up-list) (goto-char context-paren))
                            (forward-char (- direction))
                            (googlesql-skip-comment (- direction))
                            (point)))))))
    result))


(cl-defun googlesql-backward-list-item (&optional
                                        (n 1)
                                        (start-limit nil s-start-limit)
                                        (end-limit nil s-end-limit)
                                        &key
                                        (context-paren nil s-context-paren)
                                        (context-boc nil s-context-boc)
                                        (context-clause nil s-context-clause)
                                        (context-select-clause nil s-context-select-clause))
  (interactive "p")
  (apply #'googlesql-forward-list-item
         (- (or n 1))
         `(,@(if s-start-limit `(,start-limit))
           ,@(if s-end-limit `(,end-limit))
           ,@(if s-context-paren `(:context-paren ,context-paren))
           ,@(if s-context-boc `(:context-boc ,context-boc))
           ,@(if s-context-clause `(:context-clause ,context-clause))
           ,@(if s-context-select-clause `(:context-select-clause ,context-select-clause)))))

(cl-defun googlesql-forward-bool-operator (&optional
                                           (n 1)
                                           limit
                                           &key
                                           (context-jinja (googlesql-syntax-context 'jinja)))
  (unless limit
    (save-excursion
      (if (> n 0)
          (setq limit (or
                       (googlesql--match-start-tok (googlesql-forward-clause))
                       (point-max)))
        (setq limit (or
                     (googlesql--match-start-tok (googlesql-backward-clause))
                     (cdr (googlesql-beginning-of-construct))
                     (point-max))))))

  (unless context-jinja
    (let* ((forward   (>= n 0))
           (direction (if forward 1 -1))
           (start     (point))
           found pos)
      (save-excursion
        (bigquery-safe (beginning-of-thing 'symbol) nil)
        (when (and (= (point) start)
                   (looking-at-p googlesql-conjunction-symbol-regexp))
          (bigquery-safe (forward-sexp direction) nil)
          (googlesql-skip-comment 1))

        (condition-case nil
            (while (and (if forward (< (point) limit) (> (point) limit))
                        (not (setq found (looking-at-p googlesql-conjunction-symbol-regexp))))
              (forward-sexp direction)
              (googlesql-skip-comment 1))
          (scan-error nil))

        (when found (setq pos (point))))

      (when found
        (goto-char pos)
        (when forward (forward-sexp))
        pos))))

(cl-defun googlesql-backward-bool-operator (&optional
                                            (n 1)
                                            limit
                                            &key
                                            (context-jinja nil s-context-jinja))
  (apply #'googlesql-forward-bool-operator (- n) limit
         `(,@(if s-context-jinja `(:context-jinja ,context-jinja)))))


(defun googlesql-forward-at-point (&optional n)
  (interactive "p")
  (unless n
    (setq n 1))
  (or (googlesql-forward-list-item n)))

(defun googlesql-backward-at-point (&optional n)
  (interactive "p")
  (unless n
    (setq n 1))
  (googlesql-forward-at-point (- n)))

(defun googlesql-looking-at-arglist-p ()
  "Returns t if point is looking at the start of a list of arguments for a
function or operation. nil otherwise"
  (or
   (eq (char-after) ?\[) ; array literal
   (save-excursion
     (let ((delimiter-scan-end-pos (point)))
       (and
        (eq (char-after) ?\()
        (not (bigquery-safe (backward-sexp) t))
        (not (or (looking-at-p googlesql-expr-block-predecessor-token-regexp)
                 (looking-at-p (rx symbol-start "struct" symbol-end))))
        (if (looking-at-p (googlesql-symbol-regexp-opt '("by")))
            (bigquery-safe
             (progn
               (backward-sexp)
               (not (looking-at-p
                     (googlesql-symbol-regexp-opt '("group")))))
             t)
          t)
        (not (googlesql-delimiter-reached delimiter-scan-end-pos '(?\; ?,)))
        (not (progn (googlesql-skip-comment -1)
                    (or (= (point) (point-min))
                        (googlesql-delimiter-reached delimiter-scan-end-pos '(?\; ?,))))))))))

(defun googlesql-inside-arglist-p (&optional limit)
  "Returns t if point is inside a list of arguments for a function or operation,
nil otherwise."
  (unless limit (setq limit (point-min)))

  (save-excursion
    (let ((paren (googlesql-syntax-context 'paren)))
      (while (and (> (point) limit)
                  paren (not (eq (char-after paren) ?\()))
        (goto-char paren)
        (setq paren (googlesql-syntax-context 'paren)))
      (if (and (>= (point) limit) paren)
          (progn
            (goto-char paren)
            (googlesql-looking-at-arglist-p))
        nil))))

(defun googlesql-looking-at-struct-p ()
  (save-excursion
    (and (eq (char-after) ?\()
         (not (bigquery-safe (backward-sexp) t))
         (looking-at-p (rx symbol-start "struct" symbol-end)))))

(defun googlesql-inside-struct-p (&optional limit)
  (unless limit (setq limit (point-min)))

  (save-excursion
    (let ((paren (googlesql-syntax-context 'paren)))
      (while (and (> (point) limit)
                  paren (not (eq (char-after paren) ?\()))
        (goto-char paren)
        (setq paren (googlesql-syntax-context 'paren)))
      (if (and (>= (point) limit) paren)
          (progn
             (goto-char paren)
             (googlesql-looking-at-struct-p))
        nil))))

(cl-defun googlesql-inside-window-spec (&optional (limit (point-min))
                                                  (context-paren (googlesql-syntax-context 'paren)))
  (when context-paren
    (save-excursion
      (goto-char context-paren)
      (when (or (save-excursion (backward-sexp) (looking-at-p (rx symbol-start "over" symbol-end)))
                (equal (cadr (nth 1 (googlesql-backward-clause 1 limit))) "window")))
        context-paren)))

(defun googlesql-looking-at-differential-privacy-clause ()
  (let ((clause (googlesql-looking-at-clause)))
    (when (equal (cadr (nth 1 clause)) "with")
      clause)))

(defun googlesql-looking-at-select-continuation ()
    (let ((cont-parts `(,(rx symbol-start (or "struct" "value") symbol-end)
                        ,(rx symbol-start "as" symbol-end)
                        ,(rx symbol-start (or "all" "distinct") symbol-end)))
          privacy-start start)
      (save-excursion
        (dolist (part cont-parts)
          (when (looking-at-p part)
            (setq start (point))
            (bigquery-safe (backward-sexp) nil)))

        (when (setq privacy-clause
                    (googlesql-looking-at-differential-privacy-clause))
          (goto-char (car (nth 1 privacy-clause)))
          (bigquery-safe (backward-sexp) nil))

        (and (looking-at-p (rx symbol-start "select" symbol-end)) start))))

(defun googlesql-guess-basic-syntax ()
  "Makes a best-effort attempt to describe the syntactic element(s) at the line
of point and their anchor points."
  (save-excursion
    (beginning-of-line)
    (skip-chars-forward " \t")

    (let*
        ((result-syntax nil)
         (ppss (syntax-ppss))
         (context-paren (googlesql-syntax-context 'paren ppss))
         (context-boc (save-excursion
                        (googlesql-beginning-of-construct nil context-paren)))
         (context-boc-sym (car context-boc))
         (context-boc-tok (cdr context-boc))
         (context-bos (save-excursion
                        (googlesql-beginning-of-construct t context-paren)))
         (context-bos-sym (car context-bos))
         (context-bos-tok (cdr context-bos))
         (context-inside-window-spec (googlesql-inside-window-spec 1 context-paren))
         (context-clause-limit (or context-inside-window-spec context-boc-tok))
         (context-clause (save-excursion
                           (when context-clause-limit (googlesql-backward-clause 1 context-clause-limit))))
         (context-clause-sym (googlesql--match-start-intern context-clause))
         (context-clause-tok (googlesql--match-start-tok context-clause))
         (context-select-clause context-clause)
         (context-select-clause-sym context-clause-sym)
         (context-select-clause-tok context-clause-tok)
         (case-fold-search t)
         placeholder)

      (save-excursion
        ;; keep moving up until we hit the limit or find a clause that applies to
        ;; the select statement itself
        (while (and context-select-clause-sym (memq context-select-clause-sym '(on using over)))
          (setq context-select-clause (googlesql-backward-clause 1 context-clause-limit))
          (setq context-select-clause-sym (googlesql--match-start-intern context-select-clause)
                context-select-clause-tok (googlesql--match-start-tok context-select-clause))))

      (cond
       ;; inside a comment
       ((googlesql-syntax-context 'comment ppss)
        (push `(comment-continuation . ,(googlesql-syntax-context 'comment))
              result-syntax))

       ;; inside a string
       ((googlesql-syntax-context 'string)
        (beginning-of-line 0)
        (push `(string . ,(point)) result-syntax))

       ;; jinja-specific rules
       ((googlesql-syntax-context 'jinja ppss)
        )

       ;; statement intros
       ((and (= (point) context-bos-tok)
             (not context-inside-window-spec))
        (save-excursion
          (if context-paren
              (let ((sym (char-after)))
                (googlesql-skip-comment -1)
                (backward-char)
                (when (memq (char-after) '(?\( ?\[))
                  (push
                   (cons (cond ((and (eq sym ?\))
                                     (googlesql-looking-at-arglist-p))
                                'arglist-close)
                               ((eq sym ?\)) 'expr-block-close)
                               ((googlesql-looking-at-arglist-p) 'arglist-intro)
                               ((googlesql-looking-at-struct-p) 'select-list)
                               (t 'expr-block-intro))
                         (googlesql-point 'boi))
                   result-syntax)))

            (googlesql-skip-comment -1)
            (beginning-of-line)
            (push `(topmost-intro . ,(point)) result-syntax)))

        (when (eq (char-after) ?\()
          (push '(expr-block-open . nil) result-syntax)))

       ;; subqueries, set operators
       ((and (= (point) context-boc-tok)
             (not context-inside-window-spec))
        (save-excursion
          (goto-char context-bos-tok)
          (push `(statement-construct . ,(googlesql-point 'boi)) result-syntax)))

       ;; at clause start
       ((setq placeholder (googlesql-looking-at-clause :context-paren context-paren
                                                       :anywhere nil))
        (let ((sym (googlesql--match-start-intern placeholder))
              extra)
          ;; determine anchor
          (push
           (cons
            (alist-get sym googlesql-clause-symbol-alist nil)
            (cond
             ;; on/using clauses should be anchored to nearest join, if found
             ((memq sym '(on using))
              (googlesql-point 'boi
                               (or (googlesql--match-start-tok (googlesql-backward-join
                                                                1 context-boc-tok))
                                   context-select-clause-tok
                                   context-clause-tok
                                   context-boc-tok)))
             ;; over clause should anchor on function start
             ((eq sym 'over)
              (bigquery-safe
               (progn
                 (backward-sexp)
                 (skip-syntax-backward "w._")
                 (googlesql-point 'boi))
               context-boc-tok))
             ;; window clauses should anchor to open
             ((and (memq sym '(partition order))
                   context-inside-window-spec
                   context-paren)
              (when (= (point) context-boc-tok)
                (setq extra `(window-spec-intro . ,(googlesql-point
                                                    'boi (or context-paren
                                                             context-boc-tok)))))
              context-boc-tok)
             ;; normal clause case: anchor to construct
             (t context-boc-tok)))
           result-syntax)
          (when extra (push extra result-syntax))))

       ;; at join operation keyword
       ((and (setq placeholder (googlesql-looking-at-join))
             (or context-select-clause-tok context-boc-tok))
        (push
         (if (eq (point) (googlesql--match-start-tok placeholder))
             `(join-operation . ,(googlesql-point
                                  'boi (or context-select-clause-tok context-boc-tok)))
           `(join-continuation . ,(googlesql-point
                                   'boi (googlesql--match-start-tok placeholder))))
         result-syntax))

       ;; not a clause symbol, statement intro, subquery/set operation, and
       ;; contained by a boolean clause
       ((and (or (eq context-clause-sym 'on)
                 (memq context-select-clause-sym '(where having qualify)))
             (not (and context-paren (eq (char-after) ?\)))))
        (let* ((relevant-clause-tok (if (eq context-clause-sym 'on)
                                        context-clause-tok
                                      context-select-clause-tok))
               (prev-tok (googlesql-point 'backsexp))
               (connected (save-excursion (googlesql-backward-bool-operator
                                           1 context-select-clause-tok :context-jinja nil)))
               (intro (or (and connected (= (point) connected))
                          (and connected (= prev-tok connected))
                          (= prev-tok relevant-clause-tok))))

          ;; after the beginning of the clause, or connected logically by an
          ;; AND or OR operator
          (if intro
              (progn
                (push `(clause-bool-expression . ,(or connected relevant-clause-tok))
                      result-syntax)
                (when (eq (char-after) ?\()
                  (push '(expr-block-open . nil) result-syntax)))
            (push `(expression-continuation . ,(or (bigquery-safe (googlesql-point 'boi prev-tok) nil)
                                                   relevant-clause-tok))
                  result-syntax))))

       ((googlesql-inside-arglist-p)
        (let ((open-anchor (googlesql-point 'boi context-paren))
              (intro-anchor (googlesql-point 'fw (1+ context-paren)))
              cont-anchor)

          (cond ((eq (char-after) ?\))
                 (push `(arglist-close . ,open-anchor) result-syntax))
                ((or (eq (char-after) ?\,)
                     (save-excursion
                       (bigquery-safe
                        (progn
                          (googlesql-skip-comment -1)
                          (setq cont-anchor (googlesql-point 'boi))
                          (eq (char-before) ?\,))
                        nil)))
                 (push `(arglist-continuation . ,intro-anchor) result-syntax))
                (t (push `(arglist-expr-continuation . ,cont-anchor)
                         result-syntax)))))

       ;; window-spec close
       ((and context-inside-window-spec context-paren (eq (char-after) ?\)))
        (push `(window-spec-close . ,(googlesql-point 'boi context-paren))
              result-syntax))

       ;; expression block close
       ((and context-paren (eq (char-after) ?\)))
        (push `(expr-block-close . ,(googlesql-point 'boi context-paren))
              result-syntax))

       ;; inside a select list / struct definition
       ((or (and (eq context-boc-sym 'select)
                 (or (null context-select-clause-tok)
                     (eq context-select-clause-sym 'with)))
            (googlesql-inside-struct-p))
        (let (after-comma back-item)
          (save-excursion
            (cond ((googlesql-looking-at-differential-privacy-clause)
                   ;; at differential privacy clause
                   (push `(differential-privacy-clause . ,context-boc-tok)
                         result-syntax))
                  ((googlesql-looking-at-select-continuation)
                   ;; keywords like AS STRUCT/VALUE, DISTINCT, etc after SELECT
                   (push `(select-continuation . ,context-boc-tok)
                         result-syntax))
                  ;; starts with comma, comes after comma, or comes after select
                  ;; preamble
                  (t
                   (setq after-comma (or (eq (char-after) ?,)
                                         (eq (char-before (googlesql-point 'bw)) ?,))
                         back-item (save-excursion
                                     (googlesql-backward-list-item
                                      1 context-boc-tok (point-max)
                                      :context-paren context-paren
                                      :context-boc context-boc
                                      :context-clause context-clause
                                      :context-select-clause context-select-clause)))
                   (push
                    (if (or after-comma (null back-item))
                            `(select-list . ,(googlesql-point 'boi context-boc-tok))
                        `(select-list-continuation . ,back-item))
                    result-syntax))))))

       ;; item inside a from clause
       ((eq context-select-clause-sym 'from)
        (let ((join-start (save-excursion
                            (googlesql--match-start-tok
                             (googlesql-backward-join 1 context-select-clause-tok)))))
          (push
           `(from-item . ,(googlesql-point 'boi (or join-start context-select-clause-tok)))
           result-syntax)))

       ((memq context-select-clause-sym '(group order partition))
        (let ((after-comma (or (eq (char-after) ?,) (eq (googlesql-point 'bw) ?,)))
              (back-item (save-excursion (googlesql-backward-list-item)))
              intro delimited expr-start)
          (if (or after-comma (null back-item))
              (progn
                (push `(clause-list-item . ,context-select-clause-tok) result-syntax)
                (when (eq (char-after) ?\()
                  (push '(expr-block-open . nil) result-syntax)))
            (push `(expression-continuation . ,back-item) result-syntax))))

       ;; common table expression
       ((and (= context-bos-tok context-boc-tok) (eq context-bos-sym 'with))
        (push `(cte-item . ,(googlesql-point 'boi context-bos-tok))
              result-syntax))

       ;; inside a window-spec without a clause
       ((and context-paren context-inside-window-spec)
        (push `(window-spec-item . ,(googlesql-point 'boi context-paren))
              result-syntax))

       ;; inside an expression block without a construct
       ((and context-paren (null context-boc-sym))
        (save-excursion
          (goto-char (1+ context-paren))
          (googlesql-skip-comment 1)
          (push `(expr-block-continuation . ,(point)) result-syntax))))

      result-syntax)))

(defun googlesql-show-syntactic-information ()
  (interactive)
  (let ((syntax (googlesql-guess-basic-syntax))
        elem pos ol)
    (message "Syntactic analysis: %s" syntax)
    (while syntax
      (setq elem (pop syntax)
            pos  (cdr elem))
      (when pos
        (setq ol (make-overlay pos (1+ pos)))
        (overlay-put ol 'face 'highlight)
        (sit-for 10)
        (delete-overlay ol)))))

(defun googlesql-indent-line-function (&optional syntax)
  (unless syntax
    (setq syntax (googlesql-guess-basic-syntax)))

  (let (anchor indent syntax-elem offset)
    (dolist (elem syntax)
      (setq syntax-elem (or syntax-elem (car elem))
            anchor (or anchor (cdr elem))))
    (unless anchor
      (setq anchor (save-excursion
                     (bigquery-safe
                      (progn
                        (previous-line)
                        (beginning-of-line)
                        (skip-chars-forward " \t")
                        (point))
                      (point-min)))))
    (save-excursion
      (goto-char anchor)
      (setq indent (current-column)))
    (setq offset (alist-get syntax-elem googlesql-offsets-alist))
    (setq offset
          (cond ((and (symbolp offset) (memq offset '(+ ++ - --)))
                 (pcase offset
                   ('+  googlesql-basic-offset)
                   ('++ (* 2 googlesql-basic-offset))
                   ('-  (- googlesql-basic-offset))
                   ('-- (- (* 2 googlesql-basic-offset)))))
                ((and (functionp offset) offset)
                 (offset `(,syntax-elem . ,anchor)))
                ((integerp offset)
                 offset)
                (t 0)))
    (if (and (memq (char-after) '(?\n ?\r))
             (save-excursion
               (skip-chars-backward " \t")
               (= (current-column) 0)))
        (indent-line-to (max 0 (+ indent offset)))
      (save-excursion
        (indent-line-to (max 0 (+ indent offset)))))))

(define-derived-mode googlesql-mode prog-mode "GoogleSQL"
  :syntax-table googlesql-syntax-table
  (setq font-lock-defaults `(,googlesql-font-lock-keywords nil t nil))
  (setq indent-line-function #'googlesql-indent-line-function)
  (setq-local electric-indent-chars (seq-union '(?\n ?\r ?\( ?\))
                                               (mapcar
                                                (lambda (elem)
                                                  (string-to-char (substring elem -1)))
                                                (append googlesql-construct-symbols
                                                        googlesql-construct-clause-symbols
                                                        googlesql-conjunction-symbols
                                                        googlesql-expr-block-predecessor-tokens))))
  (with-eval-after-load 'evil
    (evil-define-key '(normal visual) googlesql-mode-map "gl" #'googlesql-forward-at-point)
    (evil-define-key '(normal visual) googlesql-mode-map "gh" #'googlesql-backward-at-point)))

(provide 'bigquery)
