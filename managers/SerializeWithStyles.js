Element.prototype.serializeWithStyles = (function () {  

    // Mapping between tag names and css default values lookup tables. This allows to exclude default values in the result.
    var defaultStylesByTagName = {};

    // Styles inherited from style sheets will not be rendered for elements with these tag names
    var noStyleTags = {"BASE":true,"HEAD":true,"HTML":true,"META":true,"NOFRAME":true,"NOSCRIPT":true,"PARAM":true,"SCRIPT":true,"STYLE":true,"TITLE":true};

    // This list determines which css default values lookup tables are precomputed at load time
    // Lookup tables for other tag names will be automatically built at runtime if needed

    // Precompute the lookup tables.


    return function serializeWithStyles() {
        if (this.nodeType !== Node.ELEMENT_NODE) { throw new TypeError(); }
        var cssTexts = [];
        var cssText;
        var elements = this.querySelectorAll("*");

        if (!noStyleTags[this.tagName]) {
            var computedStyle = getComputedStyle(this);
            cssText = this.style.cssText;
            for (var ii = 0; ii < computedStyle.length; ii++) {
                var cssPropName = computedStyle[ii];
                    this.style[cssPropName] = computedStyle[cssPropName];
            }
        }

        for ( var i = 0; i < elements.length; i++ ) {
            var e = elements[i];
            if (!noStyleTags[e.tagName]) {
                var computedStyle = getComputedStyle(e);
                cssTexts[i] = e.style.cssText;
                for (var ii = 0; ii < computedStyle.length; ii++) {
                    var cssPropName = computedStyle[ii];
                        e.style[cssPropName] = computedStyle[cssPropName];
                }
            }
        }
        var result = this.outerHTML;
        console.log(this)
        this.style.cssText = cssText;
        for ( var i = 0; i < elements.length; i++ ) {
            elements[i].style.cssText = cssTexts[i];
        }
        return result;
    }
})();
