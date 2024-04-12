import{S as at,i as ht,s as rt,e as X,t as lt,a as ct,B,b as O,d as Y,f as et,n as pt,U as st,o as $,q as nt,_ as ut,r as dt,u as gt,F as ot,y as mt}from"./index-8239faad.js";import"./pt-4424e41e.js";(function(){function m(t,e){for(var i in e)U.call(e,i)&&(t[i]=e[i]);function s(){this.constructor=t}return s.prototype=e.prototype,t.prototype=new s,t.__super__=e.prototype,t}var x,f,S,_,v,V,I,u,g,A,M,R,p=[].slice,U={}.hasOwnProperty,z=[].indexOf||function(t){for(var e=0,i=this.length;e<i;e++)if(e in this&&this[e]===t)return e;return-1};function G(t,e){t==null&&(t=!0),this.clear=e==null||e,t&&AnimationUpdater.add(this)}function d(){return d.__super__.constructor.apply(this,arguments)}function q(t,e){this.el=t,this.fractionDigits=e}function T(t,e){if(this.elem=t,this.text=e!=null&&e,T.__super__.constructor.call(this),this.elem===void 0)throw new Error("The element isn't defined.");this.value=1*this.elem.innerHTML,this.text&&(this.value=0)}function b(t){if(this.gauge=t,this.gauge===void 0)throw new Error("The element isn't defined.");this.ctx=this.gauge.ctx,this.canvas=this.gauge.canvas,b.__super__.constructor.call(this,!1,!1),this.setOptions()}function r(t){var e,i;this.canvas=t,r.__super__.constructor.call(this),this.percentColors=null,typeof G_vmlCanvasManager<"u"&&(this.canvas=window.G_vmlCanvasManager.initElement(this.canvas)),this.ctx=this.canvas.getContext("2d"),e=this.canvas.clientHeight,i=this.canvas.clientWidth,this.canvas.height=e,this.canvas.width=i,this.gp=[new v(this)],this.setOptions()}function k(t){this.canvas=t,k.__super__.constructor.call(this),typeof G_vmlCanvasManager<"u"&&(this.canvas=window.G_vmlCanvasManager.initElement(this.canvas)),this.ctx=this.canvas.getContext("2d"),this.setOptions(),this.render()}function D(){return D.__super__.constructor.apply(this,arguments)}(function(){var t,e,i,s,n,o,h;for(i=0,n=(h=["ms","moz","webkit","o"]).length;i<n&&(o=h[i],!window.requestAnimationFrame);i++)window.requestAnimationFrame=window[o+"RequestAnimationFrame"],window.cancelAnimationFrame=window[o+"CancelAnimationFrame"]||window[o+"CancelRequestAnimationFrame"];t=null,s=0,e={},requestAnimationFrame?window.cancelAnimationFrame||(t=window.requestAnimationFrame,window.requestAnimationFrame=function(a,l){var c;return c=++s,t(function(){if(!e[c])return a()},l),c},window.cancelAnimationFrame=function(a){return e[a]=!0}):(window.requestAnimationFrame=function(a,l){var c,W,F,y;return c=new Date().getTime(),y=Math.max(0,16-(c-F)),W=window.setTimeout(function(){return a(c+y)},y),F=c+y,W},window.cancelAnimationFrame=function(a){return clearTimeout(a)})})(),R=function(t){var e,i;for(t-=3600*(e=Math.floor(t/3600))+60*(i=Math.floor((t-3600*e)/60)),t+="",i+="";i.length<2;)i="0"+i;for(;t.length<2;)t="0"+t;return(e=e?e+":":"")+i+":"+t},A=function(){var t,e,i;return i=(e=1<=arguments.length?p.call(arguments,0):[])[0],t=e[1],u(i.toFixed(t))},M=function(t,e){var i,s,n;for(i in s={},t)U.call(t,i)&&(n=t[i],s[i]=n);for(i in e)U.call(e,i)&&(n=e[i],s[i]=n);return s},u=function(t){var e,i,s,n;for(s=(i=(t+="").split("."))[0],n="",1<i.length&&(n="."+i[1]),e=/(\d+)(\d{3})/;e.test(s);)s=s.replace(e,"$1,$2");return s+n},g=function(t){return t.charAt(0)==="#"?t.substring(1,7):t},G.prototype.animationSpeed=32,G.prototype.update=function(t){var e;return t==null&&(t=!1),!(!t&&this.displayedValue===this.value||(this.ctx&&this.clear&&this.ctx.clearRect(0,0,this.canvas.width,this.canvas.height),e=this.value-this.displayedValue,Math.abs(e/this.animationSpeed)<=.001?this.displayedValue=this.value:this.displayedValue=this.displayedValue+e/this.animationSpeed,this.render(),0))},m(d,I=G),d.prototype.displayScale=1,d.prototype.forceUpdate=!0,d.prototype.setTextField=function(t,e){return this.textField=t instanceof V?t:new V(t,e)},d.prototype.setMinValue=function(t,e){var i,s,n,o,h;if(this.minValue=t,e==null&&(e=!0),e){for(this.displayedValue=this.minValue,h=[],s=0,n=(o=this.gp||[]).length;s<n;s++)i=o[s],h.push(i.displayedValue=this.minValue);return h}},d.prototype.setOptions=function(t){return t==null&&(t=null),this.options=M(this.options,t),this.textField&&(this.textField.el.style.fontSize=t.fontSize+"px"),.5<this.options.angle&&(this.options.angle=.5),this.configDisplayScale(),this},d.prototype.configDisplayScale=function(){var t,e,i,s,n;return s=this.displayScale,this.options.highDpiSupport===!1?delete this.displayScale:(e=window.devicePixelRatio||1,t=this.ctx.webkitBackingStorePixelRatio||this.ctx.mozBackingStorePixelRatio||this.ctx.msBackingStorePixelRatio||this.ctx.oBackingStorePixelRatio||this.ctx.backingStorePixelRatio||1,this.displayScale=e/t),this.displayScale!==s&&(n=this.canvas.G__width||this.canvas.width,i=this.canvas.G__height||this.canvas.height,this.canvas.width=n*this.displayScale,this.canvas.height=i*this.displayScale,this.canvas.style.width=n+"px",this.canvas.style.height=i+"px",this.canvas.G__width=n,this.canvas.G__height=i),this},d.prototype.parseValue=function(t){return t=parseFloat(t)||Number(t),isFinite(t)?t:0},f=d,q.prototype.render=function(t){return this.el.innerHTML=A(t.displayedValue,this.fractionDigits)},V=q,m(T,I),T.prototype.displayedValue=0,T.prototype.value=0,T.prototype.setVal=function(t){return this.value=1*t},T.prototype.render=function(){var t;return t=this.text?R(this.displayedValue.toFixed(0)):u(A(this.displayedValue)),this.elem.innerHTML=t},m(b,I),b.prototype.displayedValue=0,b.prototype.value=0,b.prototype.options={strokeWidth:.035,length:.1,color:"#000000",iconPath:null,iconScale:1,iconAngle:0},b.prototype.img=null,b.prototype.setOptions=function(t){if(t==null&&(t=null),this.options=M(this.options,t),this.length=2*this.gauge.radius*this.gauge.options.radiusScale*this.options.length,this.strokeWidth=this.canvas.height*this.options.strokeWidth,this.maxValue=this.gauge.maxValue,this.minValue=this.gauge.minValue,this.animationSpeed=this.gauge.animationSpeed,this.options.angle=this.gauge.options.angle,this.options.iconPath)return this.img=new Image,this.img.src=this.options.iconPath},b.prototype.render=function(){var t,e,i,s,n,o,h,a,l;if(t=this.gauge.getAngle.call(this,this.displayedValue),a=Math.round(this.length*Math.cos(t)),l=Math.round(this.length*Math.sin(t)),o=Math.round(this.strokeWidth*Math.cos(t-Math.PI/2)),h=Math.round(this.strokeWidth*Math.sin(t-Math.PI/2)),e=Math.round(this.strokeWidth*Math.cos(t+Math.PI/2)),i=Math.round(this.strokeWidth*Math.sin(t+Math.PI/2)),this.ctx.beginPath(),this.ctx.fillStyle=this.options.color,this.ctx.arc(0,0,this.strokeWidth,0,2*Math.PI,!1),this.ctx.fill(),this.ctx.beginPath(),this.ctx.moveTo(o,h),this.ctx.lineTo(a,l),this.ctx.lineTo(e,i),this.ctx.fill(),this.img)return s=Math.round(this.img.width*this.options.iconScale),n=Math.round(this.img.height*this.options.iconScale),this.ctx.save(),this.ctx.translate(a,l),this.ctx.rotate(t+Math.PI/180*(90+this.options.iconAngle)),this.ctx.drawImage(this.img,-s/2,-n/2,s,n),this.ctx.restore()},v=b,m(r,f),r.prototype.elem=null,r.prototype.value=[20],r.prototype.maxValue=80,r.prototype.minValue=0,r.prototype.displayedAngle=0,r.prototype.displayedValue=0,r.prototype.lineWidth=40,r.prototype.paddingTop=.1,r.prototype.paddingBottom=.1,r.prototype.percentColors=null,r.prototype.options={colorStart:"#6fadcf",colorStop:void 0,gradientType:0,strokeColor:"#e0e0e0",pointer:{length:.8,strokeWidth:.035,iconScale:1},angle:.15,lineWidth:.44,radiusScale:1,fontSize:40,limitMax:!1,limitMin:!1},r.prototype.setOptions=function(t){var e,i,s,n,o;for(t==null&&(t=null),r.__super__.setOptions.call(this,t),this.configPercentColors(),this.extraPadding=0,this.options.angle<0&&(n=Math.PI*(1+this.options.angle),this.extraPadding=Math.sin(n)),this.availableHeight=this.canvas.height*(1-this.paddingTop-this.paddingBottom),this.lineWidth=this.availableHeight*this.options.lineWidth,this.radius=(this.availableHeight-this.lineWidth/2)/(1+this.extraPadding),this.ctx.clearRect(0,0,this.canvas.width,this.canvas.height),i=0,s=(o=this.gp).length;i<s;i++)(e=o[i]).setOptions(this.options.pointer),e.render();return this.render(),this},r.prototype.configPercentColors=function(){var t,e,i,s,n,o,h;if(this.percentColors=null,this.options.percentColors!==void 0){for(this.percentColors=new Array,o=[],i=s=0,n=this.options.percentColors.length-1;0<=n?s<=n:n<=s;i=0<=n?++s:--s)h=parseInt(g(this.options.percentColors[i][1]).substring(0,2),16),e=parseInt(g(this.options.percentColors[i][1]).substring(2,4),16),t=parseInt(g(this.options.percentColors[i][1]).substring(4,6),16),o.push(this.percentColors[i]={pct:this.options.percentColors[i][0],color:{r:h,g:e,b:t}});return o}},r.prototype.set=function(t){var e,i,s,n,o,h,a,l,c;for(t instanceof Array||(t=[t]),i=s=0,a=t.length-1;0<=a?s<=a:a<=s;i=0<=a?++s:--s)t[i]=this.parseValue(t[i]);if(t.length>this.gp.length)for(i=n=0,l=t.length-this.gp.length;0<=l?n<l:l<n;i=0<=l?++n:--n)(e=new v(this)).setOptions(this.options.pointer),this.gp.push(e);else t.length<this.gp.length&&(this.gp=this.gp.slice(this.gp.length-t.length));for(h=i=0,o=t.length;h<o;h++)(c=t[h])>this.maxValue?this.options.limitMax?c=this.maxValue:this.maxValue=c+1:c<this.minValue&&(this.options.limitMin?c=this.minValue:this.minValue=c-1),this.gp[i].value=c,this.gp[i++].setOptions({minValue:this.minValue,maxValue:this.maxValue,angle:this.options.angle});return this.value=Math.max(Math.min(t[t.length-1],this.maxValue),this.minValue),AnimationUpdater.add(this),AnimationUpdater.run(this.forceUpdate),this.forceUpdate=!1},r.prototype.getAngle=function(t){return(1+this.options.angle)*Math.PI+(t-this.minValue)/(this.maxValue-this.minValue)*(1-2*this.options.angle)*Math.PI},r.prototype.getColorForPercentage=function(t,e){var i,s,n,o,h,a,l;if(t===0)i=this.percentColors[0].color;else for(i=this.percentColors[this.percentColors.length-1].color,n=o=0,a=this.percentColors.length-1;0<=a?o<=a:a<=o;n=0<=a?++o:--o)if(t<=this.percentColors[n].pct){i=e===!0?(l=this.percentColors[n-1]||this.percentColors[0],s=this.percentColors[n],h=(t-l.pct)/(s.pct-l.pct),{r:Math.floor(l.color.r*(1-h)+s.color.r*h),g:Math.floor(l.color.g*(1-h)+s.color.g*h),b:Math.floor(l.color.b*(1-h)+s.color.b*h)}):this.percentColors[n].color;break}return"rgb("+[i.r,i.g,i.b].join(",")+")"},r.prototype.getColorForValue=function(t,e){var i;return i=(t-this.minValue)/(this.maxValue-this.minValue),this.getColorForPercentage(i,e)},r.prototype.renderStaticLabels=function(t,e,i,s){var n,o,h,a,l,c,W,F,y,w;for(this.ctx.save(),this.ctx.translate(e,i),c=/\d+\.?\d?/,l=(n=t.font||"10px Times").match(c)[0],F=n.slice(l.length),o=parseFloat(l)*this.displayScale,this.ctx.font=o+F,this.ctx.fillStyle=t.color||"#000000",this.ctx.textBaseline="bottom",this.ctx.textAlign="center",h=0,a=(W=t.labels).length;h<a;h++)(w=W[h]).label!==void 0?(!this.options.limitMin||w>=this.minValue)&&(!this.options.limitMax||w<=this.maxValue)&&(l=(n=w.font||t.font).match(c)[0],F=n.slice(l.length),o=parseFloat(l)*this.displayScale,this.ctx.font=o+F,y=this.getAngle(w.label)-3*Math.PI/2,this.ctx.rotate(y),this.ctx.fillText(A(w.label,t.fractionDigits),0,-s-this.lineWidth/2),this.ctx.rotate(-y)):(!this.options.limitMin||w>=this.minValue)&&(!this.options.limitMax||w<=this.maxValue)&&(y=this.getAngle(w)-3*Math.PI/2,this.ctx.rotate(y),this.ctx.fillText(A(w,t.fractionDigits),0,-s-this.lineWidth/2),this.ctx.rotate(-y));return this.ctx.restore()},r.prototype.renderTicks=function(t,e,i,s){var n,o,h,a,l,c,W,F,y,w,C,P,E,tt,L,j,Z,J,N,H;if(t!=={}){for(c=t.divisions||0,J=t.subDivisions||0,h=t.divColor||"#fff",tt=t.subColor||"#fff",a=t.divLength||.7,j=t.subLength||.2,y=parseFloat(this.maxValue)-parseFloat(this.minValue),w=parseFloat(y)/parseFloat(t.divisions),L=parseFloat(w)/parseFloat(t.subDivisions),n=parseFloat(this.minValue),o=0+L,l=(F=y/400)*(t.divWidth||1),Z=F*(t.subWidth||1),P=[],N=W=0,C=c+1;W<C;N=W+=1)this.ctx.lineWidth=this.lineWidth*a,E=this.lineWidth/2*(1-a),H=this.radius*this.options.radiusScale+E,this.ctx.strokeStyle=h,this.ctx.beginPath(),this.ctx.arc(0,0,H,this.getAngle(n-l),this.getAngle(n+l),!1),this.ctx.stroke(),o=n+L,n+=w,N!==t.divisions&&0<J?P.push((function(){var K,it,Q;for(Q=[],K=0,it=J-1;K<it;K+=1)this.ctx.lineWidth=this.lineWidth*j,E=this.lineWidth/2*(1-j),H=this.radius*this.options.radiusScale+E,this.ctx.strokeStyle=tt,this.ctx.beginPath(),this.ctx.arc(0,0,H,this.getAngle(o-Z),this.getAngle(o+Z),!1),this.ctx.stroke(),Q.push(o+=L);return Q}).call(this)):P.push(void 0);return P}},r.prototype.render=function(){var t,e,i,s,n,o,h,a,l,c,W,F,y,w,C,P;if(C=this.canvas.width/2,i=this.canvas.height*this.paddingTop+this.availableHeight-(this.radius+this.lineWidth/2)*this.extraPadding,t=this.getAngle(this.displayedValue),this.textField&&this.textField.render(this),this.ctx.lineCap="butt",c=this.radius*this.options.radiusScale,this.options.staticLabels&&this.renderStaticLabels(this.options.staticLabels,C,i,c),this.options.staticZones)for(this.ctx.save(),this.ctx.translate(C,i),this.ctx.lineWidth=this.lineWidth,s=0,o=(W=this.options.staticZones).length;s<o;s++)l=(P=W[s]).min,this.options.limitMin&&l<this.minValue&&(l=this.minValue),a=P.max,this.options.limitMax&&a>this.maxValue&&(a=this.maxValue),w=this.radius*this.options.radiusScale,P.height&&(this.ctx.lineWidth=this.lineWidth*P.height,y=this.lineWidth/2*(P.offset||1-P.height),w=this.radius*this.options.radiusScale+y),this.ctx.strokeStyle=P.strokeStyle,this.ctx.beginPath(),this.ctx.arc(0,0,w,this.getAngle(l),this.getAngle(a),!1),this.ctx.stroke();else this.options.customFillStyle!==void 0?e=this.options.customFillStyle(this):this.percentColors!==null?e=this.getColorForValue(this.displayedValue,this.options.generateGradient):this.options.colorStop!==void 0?((e=this.options.gradientType===0?this.ctx.createRadialGradient(C,i,9,C,i,70):this.ctx.createLinearGradient(0,0,C,0)).addColorStop(0,this.options.colorStart),e.addColorStop(1,this.options.colorStop)):e=this.options.colorStart,this.ctx.strokeStyle=e,this.ctx.beginPath(),this.ctx.arc(C,i,c,(1+this.options.angle)*Math.PI,t,!1),this.ctx.lineWidth=this.lineWidth,this.ctx.stroke(),this.ctx.strokeStyle=this.options.strokeColor,this.ctx.beginPath(),this.ctx.arc(C,i,c,t,(2-this.options.angle)*Math.PI,!1),this.ctx.stroke(),this.ctx.save(),this.ctx.translate(C,i);for(this.options.renderTicks&&this.renderTicks(this.options.renderTicks,C,i,c),this.ctx.restore(),this.ctx.translate(C,i),n=0,h=(F=this.gp).length;n<h;n++)F[n].update(!0);return this.ctx.translate(-C,-i)},_=r,m(k,f),k.prototype.lineWidth=15,k.prototype.displayedValue=0,k.prototype.value=33,k.prototype.maxValue=80,k.prototype.minValue=0,k.prototype.options={lineWidth:.1,colorStart:"#6f6ea0",colorStop:"#c0c0db",strokeColor:"#eeeeee",shadowColor:"#d5d5d5",angle:.35,radiusScale:1},k.prototype.getAngle=function(t){return(1-this.options.angle)*Math.PI+(t-this.minValue)/(this.maxValue-this.minValue)*(2+this.options.angle-(1-this.options.angle))*Math.PI},k.prototype.setOptions=function(t){return t==null&&(t=null),k.__super__.setOptions.call(this,t),this.lineWidth=this.canvas.height*this.options.lineWidth,this.radius=this.options.radiusScale*(this.canvas.height/2-this.lineWidth/2),this},k.prototype.set=function(t){return this.value=this.parseValue(t),this.value>this.maxValue?this.options.limitMax?this.value=this.maxValue:this.maxValue=this.value:this.value<this.minValue&&(this.options.limitMin?this.value=this.minValue:this.minValue=this.value),AnimationUpdater.add(this),AnimationUpdater.run(this.forceUpdate),this.forceUpdate=!1},k.prototype.render=function(){var t,e,i,s;return t=this.getAngle(this.displayedValue),s=this.canvas.width/2,i=this.canvas.height/2,this.textField&&this.textField.render(this),(e=this.ctx.createRadialGradient(s,i,39,s,i,70)).addColorStop(0,this.options.colorStart),e.addColorStop(1,this.options.colorStop),this.radius,this.lineWidth,this.radius,this.lineWidth,this.ctx.strokeStyle=this.options.strokeColor,this.ctx.beginPath(),this.ctx.arc(s,i,this.radius,(1-this.options.angle)*Math.PI,(2+this.options.angle)*Math.PI,!1),this.ctx.lineWidth=this.lineWidth,this.ctx.lineCap="round",this.ctx.stroke(),this.ctx.strokeStyle=e,this.ctx.beginPath(),this.ctx.arc(s,i,this.radius,(1-this.options.angle)*Math.PI,t,!1),this.ctx.stroke()},m(D,x=k),D.prototype.strokeGradient=function(t,e,i,s){var n;return(n=this.ctx.createRadialGradient(t,e,i,t,e,s)).addColorStop(0,this.options.shadowColor),n.addColorStop(.12,this.options._orgStrokeColor),n.addColorStop(.88,this.options._orgStrokeColor),n.addColorStop(1,this.options.shadowColor),n},D.prototype.setOptions=function(t){var e,i,s,n;return t==null&&(t=null),D.__super__.setOptions.call(this,t),n=this.canvas.width/2,e=this.canvas.height/2,i=this.radius-this.lineWidth/2,s=this.radius+this.lineWidth/2,this.options._orgStrokeColor=this.options.strokeColor,this.options.strokeColor=this.strokeGradient(n,e,i,s),this},S=D,window.AnimationUpdater={elements:[],animId:null,addAll:function(t){var e,i,s,n;for(n=[],i=0,s=t.length;i<s;i++)e=t[i],n.push(AnimationUpdater.elements.push(e));return n},add:function(t){if(z.call(AnimationUpdater.elements,t)<0)return AnimationUpdater.elements.push(t)},run:function(t){var e,i,s,n,o,h,a;if(t==null&&(t=!1),isFinite(parseFloat(t))||t===!0){for(e=!0,a=[],s=i=0,o=(h=AnimationUpdater.elements).length;i<o;s=++i)h[s].update(t===!0)?e=!1:a.push(s);for(n=a.length-1;0<=n;n+=-1)s=a[n],AnimationUpdater.elements.splice(s,1);return AnimationUpdater.animId=e?null:requestAnimationFrame(AnimationUpdater.run)}if(t===!1)return AnimationUpdater.animId===!0&&cancelAnimationFrame(AnimationUpdater.animId),AnimationUpdater.animId=requestAnimationFrame(AnimationUpdater.run)}},typeof window.define=="function"&&window.define.amd!=null?define(function(){return{Gauge:_,Donut:S,BaseDonut:x,TextRenderer:V}}):typeof module<"u"&&module.exports!=null?module.exports={Gauge:_,Donut:S,BaseDonut:x,TextRenderer:V}:(window.Gauge=_,window.Donut=S,window.BaseDonut=x,window.TextRenderer=V)}).call(globalThis);function ft(m){let x,f,S,_,v,V,I;return{c(){var u,g;x=X("span"),f=lt(m[1]),S=ct(),_=X("div"),v=X("canvas"),B(x,"display","none"),O(v,"id",m[2]),O(v,"width",V=m[3]||"100%"),O(v,"height",I=((u=m[0])==null?void 0:u.height)||"200px"),B(_,"width",m[3]||"100%"),B(_,"height",((g=m[0])==null?void 0:g.height)||"200px")},m(u,g){Y(u,x,g),et(x,f),Y(u,S,g),Y(u,_,g),et(_,v)},p(u,[g]){var A,M;g&2&&pt(f,u[1]),g&4&&O(v,"id",u[2]),g&8&&V!==(V=u[3]||"100%")&&O(v,"width",V),g&1&&I!==(I=((A=u[0])==null?void 0:A.height)||"200px")&&O(v,"height",I),g&8&&B(_,"width",u[3]||"100%"),g&1&&B(_,"height",((M=u[0])==null?void 0:M.height)||"200px")},i:st,o:st,d(u){u&&($(x),$(S),$(_))}}}function xt(m,x,f){let S,_;nt(m,ut,p=>f(7,S=p)),nt(m,dt,p=>f(8,_=p));let{data:v=[]}=x,{options:V={}}=x,{_id:I=""}=x,{chrt_id:u=`id_${Math.floor(Math.random()*Date.now())}`}=x,{width:g="100%"}=x,A=null,M={};const R=(p,U)=>{var G;let z=mt.cloneDeep(p);return(G=p==null?void 0:p.series)==null||G.map(d=>{var q,T,b;d.value&&(d.value=((q=U.map(r=>r==null?void 0:r[d.value]))==null?void 0:q[0])||d.value),d.total&&(d.total=((T=U.map(r=>r==null?void 0:r[d.total]))==null?void 0:T[0])||d.total),d.desc_value&&(d.desc_value=((b=U.map(r=>r==null?void 0:r[d.desc_value]))==null?void 0:b[0])||d.desc_value)}),z};return gt(async()=>{_.changeLanguage(S==null?void 0:S.lang);try{ot.locale(S==null?void 0:S.lang)}catch{ot.locale("en-us")}try{setTimeout(()=>{const p=document.getElementById(u);f(5,M=R(V.options,v)),delete M.series,f(5,M={angle:.15,lineWidth:.44,radiusScale:1,pointer:{length:.6,strokeWidth:.035,color:"#000000"},limitMax:!1,limitMin:!1,colorStart:"#6FADCF",colorStop:"#8FC0DA",strokeColor:"#E0E0E0",generateGradient:!0,highDpiSupport:!0}),A=window.Gauge(p).setOptions(M),A.maxValue=3e3,A.setMinValue(0),A.animationSpeed=32,A.set(1250)},500)}catch(p){console.log("Err. Gauge.js Engine",p==null?void 0:p.message)}}),m.$$set=p=>{"data"in p&&f(4,v=p.data),"options"in p&&f(0,V=p.options),"_id"in p&&f(1,I=p._id),"chrt_id"in p&&f(2,u=p.chrt_id),"width"in p&&f(3,g=p.width)},m.$$.update=()=>{m.$$.dirty&49&&(f(5,M=R(V.options,v)),f(5,M={...M}),console.log({opts:M,data:v}))},[V,I,u,g,v,M]}class wt extends at{constructor(x){super(),ht(this,x,xt,ft,rt,{data:4,options:0,_id:1,chrt_id:2,width:3})}}export{wt as default};
