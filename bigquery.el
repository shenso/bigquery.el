;; bigquery.el - BigQuery modes for GNU Emacs -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Shawn Henson

;; Author: Shawn Henson <shawn@shenso.name>
;; Version: 0.1
;; Package-Requires: ()
;; Keywords: sql, bigquery
;; URL: https://github.com/shenso/bigquery.el

;;; Commentary:

;; This package provides major modes for working with GoogleSQL and interacting
;; with BigQuery projects, datasets, and tables.

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

(defconst googlesql-construct-symbols
  '("with" "select" "create" "alter" "drop" "undrop" "insert" "delete" "truncate"
    "update" "merge" "grant" "revoke" "declare" "set" "execute" "begin"
    "exception" "end" "if" "loop" "repeat" "while" "break" "leave" "continue"
    "iterate" "for" "commit" "rollback" "raise" "return" "call" "export" "load"
    "assert" "union"))

(defconst googlesql-construct-clause-symbols
  '("with" "from" "where" "group" "having" "qualify" "window" "on"))

(defconst googlesql-conjunction-symbols
  '("and" "or"))

(defconst googlesql-expr-block-predecessor-tokens
  '("with" "select" "from" "where" "having" "qualify" "window" "on" "as"
    "over" "and" "or" "not"))

(defconst googlesql-clause-symbol-alist
  '((with    . differential-privacy-clause)
    (from    . from-clause)
    (where   . where-clause)
    (group   . group-by-clause)
    (having  . having-clause)
    (qualify . qualify-clause)
    (window  . window-clause)
    (on      . on-clause)
    (using   . using-clause)))

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

(defun googlesql-build-font-lock-keyword (keywords face)
  (cons (googlesql-symbol-regexp-opt keywords) face))

(defvar googlesql-font-lock-keywords
  `(,(googlesql-build-font-lock-keyword googlesql-builtin-functions
                                        'font-lock-builtin-face)
    ,(googlesql-build-font-lock-keyword googlesql-reserved-keywords
                                        'font-lock-keyword-face)))

(defcustom googlesql-basic-offset 4
  "The offset used in indentation")

(defcustom googlesql-offsets-alist
  `((topmost-intro               . 0)
    (string                      . 0)
    (comment-continuation        . 0)
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
    (differential-privacy-clause . +)
    (from-clause                 . 0)
    (where-clause                . 0)
    (group-by-clause             . 0)
    (having-clause               . 0)
    (qualify-clause              . 0)
    (window-clause               . 0)
    (on-clause                   . +)
    (using-clause                . +)
    (cte-item                    . +)
    (from-item                   . +)
    (join-operation              . 0)
    (join-continuation           . 0)
    (join-continuation           . 0)
    (clause-bool-expression      . +)
    (expression-continuation     . 0)
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
    ;; single-quote strings
    (modify-syntax-entry ?' "\"" table)
    ;; quoted identifiers
    (modify-syntax-entry ?` "\"" table)
    ;; symbols -> punctuation
    (modify-syntax-entry ?&         "." table)
    (modify-syntax-entry '(?* . ?+) "." table)
    (modify-syntax-entry ?-         "." table)
    (modify-syntax-entry ?/         "." table)
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

(defun googlesql-backward-up-list (&optional ch cur-ppss)
  "Like `backward-up-list', but to a specific parentheses/brace type. Returns
the position of the opening token, if found."
  (interactive)
  (unless ch
    (setq ch ?\())
  (let* ((ppss  (or cur-ppss (syntax-ppss)))
         (start (nth 1 ppss)))
    (while (and start (not (eq (char-after start) ch)))
      (goto-char start)
      (setq ppss (syntax-ppss)
            start (nth 1 ppss)))
    (when start
      (goto-char start))
    start))

(defun googlesql-syntax-context (type)
  (let ((ppss (syntax-ppss)))
    (pcase type
      ('comment
       (and (nth 4 ppss) (nth 8 ppss)))
      ('string
       (and (nth 3 ppss) (nth 8 ppss)))
      ('paren
       (save-excursion
         (googlesql-backward-up-list ?\( ppss)))
      ('bracket
       (save-excursion
         (googlesql-backward-up-list ?{ ppss)))
      (_ nil))))

(defun googlesql-skip-comment (&optional direction)
  (let ((comment-start (googlesql-syntax-context 'comment))
        (factor (if (< (or direction 0) 0)
                    -99999
                  9999)))
    (when comment-start
      (goto-char comment-start))
    (forward-comment factor)))

(defmacro googlesql-point (type &optional point)
  (let ((sexp `(save-excursion ,@(if point `((goto-char ,point))))))
    (append sexp
            (pcase type
              ('boi `((beginning-of-line)
                      (skip-chars-forward " \t"))))
            '((point)))))

(defun googlesql-inside-jinja (&optional pos)
  "Returns non-nil if pos is inside a jinja block. If pos is not provided it
defaults to point."
  (save-excursion
    (when pos
      (goto-char pos))

    (let* ((paren-start (googlesql-syntax-context 'paren))
           (paren-ch    (char-after paren-start)))
      (while (and paren-start (not (eq paren-ch ?{)))
        (goto-char paren-start)
        (setq paren-start (googlesql-syntax-context 'paren))
        (when paren-start
          (setq paren-ch (char-after paren-start))))

      (and
       paren-start
       (eq paren-ch ?{)
       (or
        (and
         (> paren-start (point-min)) (eq (char-before paren-start) ?{))
        (and
         (< paren-start (point-max)) (eq (char-after (1+ paren-start)) ?%)))))))

(defun googlesql-region-delimited-p (start end &optional delimiters paren-level)
  "Returns the first delimiter found from start, if any."
  (unless delimiters
    (setq delimiters '(?\;)))

  (let (delimiter-pos)
    (save-excursion
      (goto-char start)
      (while (and (< (point) end)
                  (null delimiter-pos))
        (when (and (memq (char-after) delimiters)
                   (null (googlesql-syntax-context 'comment))
                   (or (null paren-level)
                       (eq paren-level
                           (or (googlesql-syntax-context 'paren) 0))))
          (setq delimiter-pos (point)))
        (forward-char)))
    delimiter-pos))

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
             
             (setq delimiter-pos
                   (googlesql-region-delimited-p (point) delimiter-scan-end-pos))
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
              (setq stopped (googlesql-region-delimited-p (point)
                                                          delimiter-scan-end-pos
                                                          delimiters))))))
      pos)))

(defun googlesql-nearest-backward-clause (&optional limit delimiters)
  "Searches for any clause symbol in the S expression behind point. Returns a
 cons cell with the symbol representing found clause and its position, if
 found."
  (unless limit
    (setq limit (point-min)))
  (unless delimiters
    (setq delimiters '(?\;)))

  (let (sym tok)
    (when (setq tok (googlesql-search-backward-sexp
                     googlesql-construct-clause-regexp limit delimiters 1))
      (setq sym (intern (downcase (match-string 0))))
      `(,sym . ,tok))))

(defmacro googlesql-js-backward-sym ()
  '(progn
     (setq prev-pos (point))
     (backward-sexp)
     (setq sym (downcase (thing-at-point 'symbol)))))

(defun googlesql-join-start ()
  "If looking at any part of a join, returns the start of the join, otherwise
 nil."
  (save-excursion
    (let* ((start-sym (bigquery-safe (downcase (thing-at-point 'symbol)) nil))
           (start-pos (point))
           (prev-pos start-pos)
           (sym start-sym))
      (cond ((equal sym "join")
             (bigquery-safe
              (progn
                (googlesql-js-backward-sym)
                (cond ((member sym '("inner" "full" "left" "right" "cross"))
                       (point))
                      ((equal sym "outer")
                       (googlesql-js-backward-sym)
                       (if (member sym '("full" "left" "right"))
                           (point)
                         ;; treat OUTER as join start, even though this is
                         ;; technically false
                         prev-pos))
                      ;; implicit inner join
                      (t start-pos)))
              ;; if we couldn't move back, then we're either at an implicit
              ;; inner join, or OUTER which we treat as the start
              (point)))
            ((equal sym "outer")
             (bigquery-safe
              (progn
                (googlesql-js-backward-sym)
                (if (member sym '("full" "left" "right"))
                    (point)
                  prev-pos))
              ;; treat OUTER as join start
              (point)))
            ((member sym '("inner" "full" "left" "right" "cross"))
             ;; these are all start symbols; we're done here.
             (point))))))

(defun googlesql-looking-at-arglist-p ()
  "Returns t if point is looking at the start of a list of arguments for a
function or operation. nil otherwise"
  (save-excursion
    (let ((delimiter-scan-end-pos (point)))
      (and
       (eq (char-after) ?\()
       (not (bigquery-safe (backward-sexp) t))
       (not (looking-at-p googlesql-expr-block-predecessor-token-regexp))
       (if (looking-at-p (googlesql-symbol-regexp-opt '("by")))
           (bigquery-safe
            (progn
              (backward-sexp)
              (not (looking-at-p
                    (googlesql-symbol-regexp-opt '("group")))))
            t)
         t)
       (not (googlesql-region-delimited-p
             (point) delimiter-scan-end-pos '(?\; ?,)))
       (not (progn (googlesql-skip-comment -1)
                   (= (point) (point-min))))))
  ))

(defun googlesql-inside-arglist-p ()
  "Returns t if point is inside a list of arguments for a function or operation,
nil otherwise."
  (save-excursion
    (let ((paren (googlesql-syntax-context 'paren)))
      (while (and paren (not (eq (char-after paren) ?\()))
        (goto-char paren))
      (if paren
          (progn
            (goto-char paren)
            (googlesql-looking-at-arglist-p))
        nil))))

(defun googlesql-looking-at-differential-privacy-clause ()
  (save-excursion
    (let ((clause-parts `(?\(
                          ,(rx symbol-start "options" symbol-end)
                          ,(rx symbol-start "differential_privacy" symbol-end)
                          ,(rx symbol-start "with" symbol-end)))
          start)

      (dolist (part clause-parts)
        (when (if (integerp part) (eq (char-after) part) (looking-at-p part))
          (setq start (point))
          (bigquery-safe (backward-sexp) nil)))

      (and (looking-at-p (rx symbol-start "select" symbol-end)) start))))

(defun googlesql-looking-at-select-continuation ()
  (save-excursion
    (let ((cont-parts `(,(rx symbol-start (or "struct" "value") symbol-end)
                        ,(rx symbol-start "as" symbol-end)
                        ,(rx symbol-start (or "all" "distinct") symbol-end)))
          privacy-start start)
      (dolist (part cont-parts)
        (when (looking-at-p part)
          (setq start (point))
          (bigquery-safe (backward-sexp) nil)))

      (when (setq privacy-start
                  (googlesql-looking-at-differential-privacy-clause))
        (goto-char privacy-start)
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
         (context-paren (googlesql-syntax-context 'paren))
         (context-boc (save-excursion
                        (googlesql-beginning-of-construct nil context-paren)))
         (context-boc-sym (car context-boc))
         (context-boc-tok (cdr context-boc))
         (context-bos (save-excursion
                        (googlesql-beginning-of-construct t context-paren)))
         (context-bos-sym (car context-bos))
         (context-bos-tok (cdr context-bos))
         (context-after-boc-tok (save-excursion
                                  (goto-char context-boc-tok)
                                  (bigquery-safe (progn (forward-sexp) (point)) nil)))
         (context-nearest-clause (when context-after-boc-tok
                                     (googlesql-nearest-backward-clause context-after-boc-tok)))
         (context-nearest-clause-sym (car context-nearest-clause))
         (context-nearest-clause-tok (cdr context-nearest-clause))
         (case-fold-search-prev case-fold-search)
         sym
         join-start)

      (setq case-fold-search t)

      (cond
       ;; inside a comment
       ((googlesql-syntax-context 'comment)
        (push `(comment-continuation . ,(googlesql-syntax-context 'comment))
              result-syntax))

       ;; inside a string
       ((googlesql-syntax-context 'string)
        (beginning-of-line 0)
        (push `(string . ,(point)) result-syntax))

       ;; jinja-specific rules
       ((googlesql-inside-jinja)
        )

       ;; statement intros
       ((= (point) context-bos-tok)
        (save-excursion
          (if context-paren
              (let ((sym (char-after)))
                (googlesql-skip-comment -1)
                (backward-char)
                (when (eq (char-after) ?\()
                  (cond ((and (eq sym ?\)) (googlesql-looking-at-arglist-p))
                         (push `(arglist-close . ,(googlesql-point boi))
                               result-syntax))
                        ((eq sym ?\))
                         (push `(expr-block-close . ,(googlesql-point boi))
                               result-syntax))
                        ((googlesql-looking-at-arglist-p)
                         (push `(arglist-intro . ,(googlesql-point boi))
                               result-syntax))
                        (t
                         (push `(expr-block-intro . ,(googlesql-point boi))
                               result-syntax)))))

            (googlesql-skip-comment -1)
            (beginning-of-line)
            (push `(topmost-intro . ,(point)) result-syntax))))

       ;; subqueries, set operators
       ((= (point) context-boc-tok)
        (save-excursion
          (goto-char context-bos-tok)
          (push `(statement-construct . ,(googlesql-point boi)) result-syntax)))

       ;; at clause start
       ((looking-at googlesql-construct-clause-regexp)
        (setq sym (intern (downcase (match-string 0))))
        (cond
         ;; on/using clauses should be anchored to nearest join, if found
         ((memq sym '(on using))
          (message "after boc tok: %d, nearest clause: %s"
                   context-after-boc-tok context-nearest-clause)
          (let (join-tok clause-anchor)
            (when context-nearest-clause
              (setq join-tok
                    (googlesql-search-backward-sexp
                     (googlesql-symbol-regexp-opt '("join"))
                     ;; if we're in FROM (we should be), do not look for a join
                     ;; past it
                     context-nearest-clause-tok
                     '(?\;)))
              (if join-tok
                  (setq clause-anchor (googlesql-point boi join-tok))
                ;; anchor on the nearest backward clause if no join operator was
                ;; found (this should be FROM)
                (setq clause-anchor context-nearest-clause-tok)))

            ;; if no join operator was found fallback on, and we're not in a
            ;; clause, fallback on the construct start as an anchor
            (unless clause-anchor
              (setq clause-anchor context-boc-tok))

            (push `(,(alist-get sym googlesql-clause-symbol-alist)
                    . ,clause-anchor)
                  result-syntax)))
         ;; normal clause case: anchor to construct
         (t
          (push `(,(alist-get sym googlesql-clause-symbol-alist)
                  . ,context-boc-tok)
                result-syntax))))

       ;; at join operation keyword
       ((and (setq join-start (googlesql-join-start))
             (or context-nearest-clause-tok context-boc-tok))
        (push
         (if (eq (point) join-start)
             `(join-operation . ,(or context-nearest-clause-tok context-boc-tok))
           `(join-continuation . ,join-start))
         result-syntax))

       ;; not a clause symbol, statement intro, subquery/set operation, and
       ;; contained by a boolean clause
       ((and context-nearest-clause
             (memq context-nearest-clause-sym '(where having qualify on)))
        (let (intro connected expr-start)
          (setq connected (looking-at-p googlesql-conjunction-symbol-regexp))
          (save-excursion
            (backward-sexp)
            (setq intro (= (point) context-nearest-clause-tok)
                  connected (or connected
                                (looking-at-p
                                 googlesql-conjunction-symbol-regexp))))

          ;; at the beginning of the statement, or connected logically by an
          ;; AND or OR operator
          (if (or intro connected)
              (progn
                (push `(clause-bool-expression . ,context-nearest-clause-tok)
                      result-syntax)
                (when (eq (char-after) ?\()
                  (push '(expr-block-open . nil) result-syntax)))
            (save-excursion
              (while (and (> (point) context-nearest-clause-tok)
                          (null expr-start))
                (backward-sexp)
                (when (or (= (point) context-nearest-clause-tok)
                          (looking-at-p googlesql-conjunction-symbol-regexp))
                  (forward-sexp)
                  (skip-chars-forward " \t\n\r")
                  (setq expr-start (googlesql-point boi)))))
            (push `(expression-continuation . ,(or expr-start
                                                   context-nearest-clause-tok))
                  result-syntax))))

       ((googlesql-inside-arglist-p)
        (let ((open-anchor (googlesql-point boi context-paren))
              (intro-anchor
               (save-excursion
                 (goto-char (1+ context-paren))
                 (googlesql-skip-comment 1)
                 (point)))
              cont-anchor)

          (cond ((eq (char-after) ?\))
                 (push `(arglist-close . ,open-anchor) result-syntax))
                ((or (eq (char-after) ?\,)
                     (save-excursion
                       (bigquery-safe
                        (progn
                          (googlesql-skip-comment -1)
                          (setq cont-anchor (googlesql-point boi))
                          (eq (char-before) ?\,))
                        nil)))
                 (push `(arglist-continuation . ,intro-anchor) result-syntax))
                (t (push `(arglist-expr-continuation . ,cont-anchor)
                         result-syntax)))))

       ;; expression block close
       ((and context-paren (eq (char-after) ?\)))
        (push `(expr-block-close . ,(googlesql-point boi context-paren))
              result-syntax))

       ;; inside a select list
       ((and (eq context-boc-sym 'select)
             (or (null context-nearest-clause-tok)
                 (> context-boc-tok context-nearest-clause-tok)
                 (eq context-nearest-clause-sym 'with)))
        (let (privacy-clause-start
              select-cont-start
              scan-delim-end-pos
              delim-pos
              elem-start)
          (save-excursion
            (cond ((googlesql-looking-at-differential-privacy-clause)
                   ;; at differential privacy clause
                   (push `(differential-privacy-clause . ,context-boc-tok)
                         result-syntax))
                  ((googlesql-looking-at-select-continuation)
                   ;; keywords like AS STRUCT/VALUE, DISTINCT, etc after SELECT
                   (push `(select-continuation . ,context-boc-tok)
                         result-syntax))
                  ;; starts with comma, comes afte comma, or comes after select
                  ;; preamble
                  ((or (eq (char-after) ?,)
                       (bigquery-safe
                        (progn
                          (setq scan-delim-end-pos (point))
                          (backward-sexp)
                          (or (= (point) context-boc-tok)
                              (googlesql-region-delimited-p (point)
                                                            scan-delim-end-pos
                                                            '(?,)
                                                            (or context-paren 0))
                              (googlesql-looking-at-select-continuation)
                              (googlesql-looking-at-differential-privacy-clause)))
                        t))
                   (push `(select-list . ,context-boc-tok) result-syntax))
                  (t
                   (while (and (> (point) context-boc-tok)
                               (catch 'loop
                                 (setq elem-start (point))
                                 (bigquery-safe (backward-sexp)
                                                (throw 'loop nil))

                                 (setq delim-pos (googlesql-region-delimited-p
                                                  (point) elem-start '(?,)
                                                  (or context-paren 0)))
                                 (when delim-pos
                                   (goto-char (1+ delim-pos))
                                   (googlesql-skip-comment 1)
                                   (setq elem-start (point))
                                   (throw 'loop nil))
                                 (when (or (looking-at-p (rx symbol-start "select"
                                                             symbol-end))
                                           (googlesql-looking-at-select-continuation)
                                           (googlesql-looking-at-differential-privacy-clause))
                                   (throw 'loop nil))
                                 t)))
                   (setq elem-start (max (googlesql-point boi elem-start)
                                         context-boc-tok))
                   (push `(select-list-continuation . ,elem-start)
                         result-syntax))))))

       ;; item inside a from clause
       ((eq context-nearest-clause-sym 'from)
        (let ((join-start (save-excursion
                            (bigquery-safe
                             (search-backward-regexp (rx symbol-start "join"
                                                         symbol-end))
                             nil))))
          (push
           `(from-item . ,(googlesql-point boi (or join-start
                                                   context-nearest-clause-tok)))
           result-syntax)))

       ;; common table expression
       ((and (= context-bos-tok context-boc-tok) (eq context-bos-sym 'with))
        (push `(cte-item . ,(googlesql-point boi context-bos-tok))
              result-syntax))

       ;; inside an expression block without a construct
       ((and context-paren (null context-boc-sym))
        (save-excursion
          (goto-char (1+ context-paren))
          (googlesql-skip-comment 1)
          (push `(expr-block-continuation . ,(point)) result-syntax)))
       )

      (setq case-fold-search case-fold-search-prev)

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
  (setq electric-indent-chars (seq-union '(?\n ?\r ?\( ?\))
                                         (mapcar
                                          (lambda (elem)
                                            (string-to-char (substring elem -1)))
                                          (append googlesql-construct-symbols
                                                  googlesql-construct-clause-symbols
                                                  googlesql-conjunction-symbols
                                                  googlesql-expr-block-predecessor-tokens)))))

(provide 'bigquery)
